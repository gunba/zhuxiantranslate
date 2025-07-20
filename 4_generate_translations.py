import json
import struct
import os
import shutil
from collections import defaultdict
import csv
import re
import unicodedata # NEWLY ADDED for accent removal

# --- Configuration ---
INSERT_DEBUG_IDS = False # When True, prepends "Namespace_ID " to translations for easy in-game identification.
TRANSLATED_UNIFIED_JSON_PATH = "./translated_unified_zxsj_data.json"
UNIFIED_LOCRES_HASH_CSV_PATH = "./unified_locres_with_hashes.csv"
KEY_SOURCE_ORIGINS_JSON_PATH = "./key_source_origins.json"
PATCH_BASE_OUTPUT_DIR = "./~Eng_Patch_P"
POSTPROCESSED_TRANSLATIONS_DEBUG_JSON_PATH = "./postprocessed_translations_for_debug.json" # NEW

LOCRES_OUTPUT_BASE = os.path.join(PATCH_BASE_OUTPUT_DIR, "ZhuxianClient", "Content", "Localization", "Game")
TEXT_FILES_OUTPUT_BASE = os.path.join(PATCH_BASE_OUTPUT_DIR, "ZhuxianClient", "gamedata", "client", "FormatString")

FS_TXT_TYPE = "FormatString_Txt"
FS_JSON_TYPE = "FormatString_Json"
UI_ASSETS_TYPE = "UI_Assets"
UI_ASSETS_PLACEHOLDER_NS = "_UI_ASSETS_TARGET_NAMESPACE_" # NEW: Namespace for UI keys saved to .txt
UI_ASSETS_FILENAME = ".txt"                         # NEW: Filename for UI keys that can't go in locres
LOCRES_TYPE = "LocRes"


# --- LocRes Constants & Helpers ---
LOCRES_MAGIC_BYTES = bytes([
    0x0E, 0x14, 0x74, 0x75, 0x67, 0x4A, 0x03, 0xFC, 0x4A, 0x15, 0x90, 0x9D, 0xC3, 0x37, 0x7F, 0x1B
])
LOCRES_VERSION_OPTIMIZED_CITYHASH_UTF16 = 0x03

# --- Utility Functions ---
def clean_key_bom(key_str: str) -> str:
    if isinstance(key_str, str) and key_str.startswith('\ufeff'):
        return key_str[1:]
    return key_str

def normalize_line_endings(text: str) -> str:
    if not isinstance(text, str): return ""
    return text.replace('\r\n', '\n').replace('\r', '\n')

def write_fstring_for_locres(stream, text: str):
    if text is None: text = ""
    text_with_null = text + '\0'
    try:
        encoded_text_bytes = text_with_null.encode('utf-16-le')
        num_utf16_chars_including_null = len(encoded_text_bytes) // 2
        length_prefix = -num_utf16_chars_including_null
        stream.write(struct.pack('<i', length_prefix))
        stream.write(encoded_text_bytes)
    except Exception as e:
        print(f"                                 ERROR encoding string to UTF-16LE: '{text[:50]}...' ({e}). Writing as empty string (length -1, null terminator).")
        stream.write(struct.pack('<i', -1))
        stream.write(b'\x00\x00')


def load_data_with_hashes_from_csv(csv_path: str):
    locres_capable_data_map = {}
    namespace_info_from_csv = {}
    print(f"  IMPORTANT ASSUMPTION: Hashes in '{csv_path}' are assumed to correspond to BOM-cleaned versions of Namespace and Key strings.")
    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            required_cols = ['Namespace', 'Key', 'SourceValue', 'NamespaceHash', 'KeyHash_of_KeyString', 'SourceStringHash_of_SourceText']
            if not reader.fieldnames or not all(h in reader.fieldnames for h in required_cols):
                missing = [h for h in required_cols if h not in (reader.fieldnames or [])]
                print(f"ERROR: CSV '{csv_path}' is missing required columns. Missing: {missing}. Found: {reader.fieldnames}")
                return None, None
            ns_order_counter = 0
            for row_num, row in enumerate(reader, 1):
                try:
                    ns_name = clean_key_bom(row['Namespace'])
                    cleaned_key_string = clean_key_bom(row['Key'])
                    if ns_name not in namespace_info_from_csv:
                        namespace_info_from_csv[ns_name] = {
                            "hash": int(row['NamespaceHash']),
                            "order": ns_order_counter
                        }
                        ns_order_counter += 1
                    elif namespace_info_from_csv[ns_name]["hash"] != int(row['NamespaceHash']):
                        print(f"WARNING: Namespace '{ns_name}' has conflicting hashes in CSV. Using first encountered.")
                    locres_capable_data_map[(ns_name, cleaned_key_string)] = {
                        'namespace_name': ns_name,
                        'key_string': cleaned_key_string,
                        'namespace_hash': namespace_info_from_csv[ns_name]["hash"],
                        'key_hash': int(row['KeyHash_of_KeyString']),
                        'source_value_from_csv': row['SourceValue'],
                        'source_string_hash': int(row['SourceStringHash_of_SourceText'])
                    }
                except KeyError as ke:
                    print(f"ERROR: Missing expected column '{ke}' in CSV row {row_num}: {row}")
                    return None, None
                except ValueError as ve:
                    print(f"ERROR: Could not parse hash as integer in CSV row {row_num}: {row} ({ve})")
                    return None, None
            total_entries_loaded = len(locres_capable_data_map)
            print(f"  Successfully loaded {total_entries_loaded} LocRes-capable entries from '{csv_path}'.")
            return locres_capable_data_map, namespace_info_from_csv
    except FileNotFoundError:
        print(f"ERROR: Unified LocRes hash CSV file not found: {csv_path}")
        return None, None
    except Exception as e:
        print(f"ERROR: Failed to read or parse unified LocRes hash CSV '{csv_path}': {e}")
        import traceback
        traceback.print_exc()
        return None, None

def generate_locres_file_v3_with_hashes(all_namespace_data: list, output_locres_path: str):
    print(f"  Generating .locres (Version {LOCRES_VERSION_OPTIMIZED_CITYHASH_UTF16}): {output_locres_path}")
    try:
        os.makedirs(os.path.dirname(output_locres_path), exist_ok=True)
        with open(output_locres_path, 'wb') as f:
            f.write(LOCRES_MAGIC_BYTES)
            f.write(struct.pack('<B', LOCRES_VERSION_OPTIMIZED_CITYHASH_UTF16))
            string_table_offset_pos = f.tell()
            f.write(struct.pack('<q', 0))
            total_key_entries = sum(len(ns_data.get("entries", [])) for ns_data in all_namespace_data)
            f.write(struct.pack('<i', total_key_entries))
            f.write(struct.pack('<i', len(all_namespace_data)))
            string_to_index_map = {}
            string_table_values = []
            for ns_data in all_namespace_data:
                for entry in ns_data.get("entries", []):
                    translated_val = entry["translated_value"]
                    if translated_val not in string_to_index_map:
                        idx = len(string_table_values)
                        string_table_values.append(translated_val)
                        string_to_index_map[translated_val] = {"index": idx, "ref_count": 1}
                    else:
                        string_to_index_map[translated_val]["ref_count"] += 1
            for ns_data in all_namespace_data:
                f.write(struct.pack('<I', ns_data["namespace_hash"]))
                write_fstring_for_locres(f, ns_data["namespace_name"])
                f.write(struct.pack('<i', len(ns_data.get("entries", []))))
                for entry in ns_data.get("entries", []):
                    f.write(struct.pack('<I', entry["key_hash"]))
                    write_fstring_for_locres(f, entry["key_string"])
                    f.write(struct.pack('<I', entry["source_string_hash"]))
                    string_index = string_to_index_map[entry["translated_value"]]["index"]
                    f.write(struct.pack('<i', string_index))
            actual_string_table_offset = f.tell()
            f.seek(string_table_offset_pos)
            f.write(struct.pack('<q', actual_string_table_offset))
            f.seek(actual_string_table_offset)
            f.write(struct.pack('<i', len(string_table_values)))
            for str_val in string_table_values:
                write_fstring_for_locres(f, str_val)
                f.write(struct.pack('<i', string_to_index_map[str_val]["ref_count"]))
            print(f"    Successfully generated .locres: {output_locres_path}")
    except Exception as e:
        print(f"    ERROR: Could not generate .locres at {output_locres_path}: {e}")
        import traceback
        traceback.print_exc()

def natural_sort_key(s: str):
    if not isinstance(s, str): return [s]
    cleaned_s = clean_key_bom(s)
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'([0-9]+)', cleaned_s)]

def write_format_string_entry_original_style(f, key, value):
    value_as_str = str(value)
    bom_cleaned_value = clean_key_bom(value_as_str)
    normalized_value = normalize_line_endings(bom_cleaned_value)
    escaped_value_for_txt = normalized_value.replace('\n', '\\n')
    line_to_write = f"{key} = {escaped_value_for_txt}"
    f.write(line_to_write + "\n")

def generate_format_string_txt_files(txt_data: dict, base_output_dir: str):
    print(f"\n--- Generating FormatString .txt files in {base_output_dir} (UTF-8, CRLF endings) ---")
    file_content_buffer = defaultdict(list)
    ROOT_KEYS_FILENAME = "_ROOT_STRINGS.txt"

    for top_level_ns, items_in_top_ns in txt_data.items():
        if not items_in_top_ns: continue

        # NEW: Handle special case for UI-only assets destined for ".txt"
        if top_level_ns == UI_ASSETS_PLACEHOLDER_NS:
            output_file_path_for_ui = os.path.normpath(os.path.join(base_output_dir, UI_ASSETS_FILENAME))
            for key_in_ui_ns, value_in_ui_ns in items_in_top_ns.items():
                 file_content_buffer[output_file_path_for_ui].append((key_in_ui_ns, value_in_ui_ns))
            continue # Skip normal processing for this special namespace

        if top_level_ns == "":
            output_file_path_for_root = os.path.normpath(os.path.join(base_output_dir, ROOT_KEYS_FILENAME))
            for key_in_root, value_in_root in items_in_top_ns.items():
                file_content_buffer[output_file_path_for_root].append((key_in_root, value_in_root))
            continue

        for complex_key, value in items_in_top_ns.items():
            path_segments_from_top_ns = list(filter(None, top_level_ns.split('/')))
            path_segments_from_complex_key = list(filter(None, complex_key.split('/')))

            target_dir_segments = path_segments_from_top_ns
            filename_stem = ""
            actual_key_in_file = ""

            if not path_segments_from_complex_key:
                print(f"                                 WARNING: Skipping FormatString .txt entry with empty complex_key (ns: '{top_level_ns}', key: '{complex_key}')")
                continue

            if len(path_segments_from_complex_key) == 1:
                actual_key_in_file = path_segments_from_complex_key[0]
                if target_dir_segments:
                    filename_stem = target_dir_segments.pop()
                else:
                    filename_stem = top_level_ns if top_level_ns else actual_key_in_file
            else:
                actual_key_in_file = path_segments_from_complex_key[-1]
                filename_stem = path_segments_from_complex_key[-2]
                target_dir_segments.extend(path_segments_from_complex_key[:-2])

            filename_stem = filename_stem.replace('/', '_')
            if not filename_stem: filename_stem = "unknown_formatstring_file"

            target_dir = os.path.join(base_output_dir, *target_dir_segments)
            output_file_path = os.path.normpath(os.path.join(target_dir, f"{filename_stem}.txt"))
            file_content_buffer[output_file_path].append((actual_key_in_file, value))

    processed_files_count = 0
    if not file_content_buffer:
        print("  No FormatString .txt data to write.")
        return

    for output_file_path, key_value_pairs in file_content_buffer.items():
        if not key_value_pairs: continue
        try:
            os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
            key_value_pairs.sort(key=lambda x: natural_sort_key(x[0]))
            with open(output_file_path, 'w', encoding='utf-8', newline='\r\n') as f:
                for key, entry_value in key_value_pairs:
                    write_format_string_entry_original_style(f, key, entry_value)
            processed_files_count += 1
        except Exception as e:
            print(f"    ERROR writing FormatString TXT {output_file_path}: {e}")
            import traceback; traceback.print_exc()
    print(f"  Generated {processed_files_count} FormatString .txt files based on content.")


def generate_format_json_files(json_data_map: dict, base_output_dir: str):
    print(f"\n--- Generating FormatString .json files in {base_output_dir} ---")
    processed_files_count = 0
    if not json_data_map:
        print("  No data for FormatString .json file generation.")
        return

    for namespace, entries_data in json_data_map.items():
        if not entries_data: continue

        path_parts = [part for part in namespace.split('/') if part]
        if not path_parts:
            output_filename = "_ROOT_STRINGS.json"
            target_dir = base_output_dir
        elif len(path_parts) == 1:
            output_filename = f"{path_parts[0]}.json"
            target_dir = base_output_dir
        else:
            output_filename = f"{path_parts[-1]}.json"
            target_dir = os.path.join(base_output_dir, *path_parts[:-1])

        output_file_path = os.path.normpath(os.path.join(target_dir, output_filename))
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        sorted_entries_for_json = dict(sorted(entries_data.items(), key=lambda item: natural_sort_key(item[0])))

        try:
            with open(output_file_path, 'w', encoding='utf-8') as f:
                json.dump(sorted_entries_for_json, f, indent=4, ensure_ascii=False)
            processed_files_count += 1
        except Exception as e:
            print(f"    ERROR writing FormatString JSON {output_file_path}: {e}")
            import traceback; traceback.print_exc()

    print(f"  Generated {processed_files_count} FormatString .json files.")

def handle_rtp_tags(input_str: str) -> str:
    """
    Heal a string containing RTP-style tags (<RTP_TagName>Text</>) by:
      1. Normalizing tag names (e.g. fixing RTp_Default → RTP_Default, <Default> → <RTP_Default>).
      2. Ensuring tags are never nested—automatically closing any unclosed tag before a new one.
      3. Removing any empty tags (tags with no text inside, or only whitespace).
      4. Moving newline characters and ampersands out of tags (closing and reopening tags around them).
      5. Collapsing any sequence of two or more spaces into a single space.
      6. Removing spaces immediately before punctuation (%, apostrophe, comma, semicolon, period, colon).
      7. Removing any space between a closing tag (</>) and an opening‐tagged apostrophe
         (e.g. `</> <RTP_Default>' → </><RTP_Default>'`), but only if those intervening chars are literal spaces.
      8. Ensuring there’s a space between any `</>` and the next `<RTP_...>` tag if no space exists,
         except when `</>` is immediately preceded by a plus (`+`)—so that:
           • “Consuming</><RTP_SkillTitleName>Endless</>” → “Consuming</> <RTP_SkillTitleName>Endless</>”
           • but “+</><RTP_SkillTitleName>2</>” remains “+</><RTP_SkillTitleName>2</>`
      9. Ensuring exactly one space between a digit or colon and the next opening `<RTP_…>` tag
         (only when not immediately preceded by `+`).
     10. **MERGE‐HYPHEN TAGS**: If two consecutive tags share the same name and the second begins with `-`,
         merge them into one (e.g. `<TAG>5</> <TAG>-minute…</>` → `<TAG>5-minute…</>`).
     11. **MERGE MULTIPLE DIGITS**: Repeatedly collapse runs of `<RTP_SkillTitleName>` each wrapping a single digit
         into one tag. For example:
           • `<RTP_SkillTitleName>1</> <RTP_SkillTitleName>2</>` → `<RTP_SkillTitleName>12</>`
           • `<RTP_SkillTitleName>1</> <RTP_SkillTitleName>2</> <RTP_SkillTitleName>3</>` → `<RTP_SkillTitleName>123</>`
     12. Removing the space just added when the tag wraps a single letter (possibly with trailing punctuation).
         (e.g. “3</> <RTP_X>s</>” or “3</> <RTP_Default>s.</>” → “3</><RTP_Default>s.</>”)
     13. Adding a space between a letter and any opening `<RTP_…>` tag if no space exists
         (so that “foo<RTP_SkillTitleName>Bar</>” → “foo <RTP_SkillTitleName>Bar</>`).
     14. Replacing any ampersand (“&”) that is not directly adjacent to a newline (`\r` or `\n`) with “and.”
     15. Removing any space immediately **after** an opening `<RTP_…>` tag.
     16. Removing any space immediately **before** a closing `</>` tag.
     17. Removing any space between `</>` and an opening‐tagged comma (e.g. `</> <RTP_Default>,` → `</><RTP_Default>,`).
     18. Removing any space between `</>` and an opening‐tagged punctuation (semicolon, period, percent, apostrophe, comma, colon, exclamation)
         **when that punctuation is the very first character inside the tag** (e.g. `</> <RTP_Default>:` → `</><RTP_Default>:`).
     19. Preserving newline characters between lines (but never inside a tag).
    """
    # 1) Split the input into tags and text, preserving tags as separate tokens
    tag_pattern = re.compile(r'(<\s*/\s*>|<\s*[^>]+>)')
    parts = tag_pattern.split(input_str)

    # 2) Classify each part as opening‐tag, closing‐tag, or plain text
    tokens = []
    for part in parts:
        if not part:
            continue

        # Closing tag: “</>” (allowing whitespace inside)
        if re.fullmatch(r'<\s*/\s*>', part):
            tokens.append({'type': 'close'})

        # Opening tag: anything that starts with "<" and ends with ">", but is not a closing tag
        elif part.startswith('<') and part.endswith('>'):
            raw_name = part[1:-1].strip()
            lower_name = raw_name.lower()

            # Normalize tag names:
            if lower_name == 'default' or lower_name == 'rtp_default':
                tagname = 'RTP_Default'
            elif lower_name.startswith('rtp_'):
                # Preserve everything after the first underscore exactly as given,
                # but force the “RTP_” prefix in uppercase.
                suffix = raw_name.split('_', 1)[1]
                tagname = 'RTP_' + suffix
            else:
                tagname = raw_name

            tokens.append({'type': 'open', 'tagname': tagname})

        else:
            # Plain text
            tokens.append({'type': 'text', 'text': part})

    # 3) Process tokens to enforce “no nesting,” handle newlines and ampersands inside tags,
    #    and remove empty tags (treating “only whitespace” as empty).
    result_tokens = []
    current_open_tag = None
    last_open_index = None
    buffer_content = ''  # accumulates text inside the currently open tag

    for tok in tokens:
        if tok['type'] == 'open':
            # If there's already an open tag, close it first (or remove it if empty/whitespace-only)
            if current_open_tag is not None:
                if buffer_content.strip() == '':
                    # Remove the previously opened tag entirely
                    del result_tokens[last_open_index]
                else:
                    # Properly close the previous tag
                    result_tokens.append({'type': 'close'})
                current_open_tag = None
                last_open_index = None
                buffer_content = ''

            # Now push the new opening tag
            result_tokens.append({'type': 'open', 'tagname': tok['tagname']})
            current_open_tag = tok['tagname']
            last_open_index = len(result_tokens) - 1
            buffer_content = ''

        elif tok['type'] == 'close':
            # If a tag is open, close it (or remove if empty/whitespace-only). Otherwise ignore stray </>.
            if current_open_tag is not None:
                if buffer_content.strip() == '':
                    del result_tokens[last_open_index]
                else:
                    result_tokens.append({'type': 'close'})
                current_open_tag = None
                last_open_index = None
                buffer_content = ''
            # else: stray </> without a matching open → ignore

        else:  # tok['type'] == 'text'
            text = tok['text']
            if current_open_tag is not None:
                # Split on newline or ampersand, keeping the delimiter
                segments = re.split(r'(\r\n|\r|\n|&)', text)
                for seg in segments:
                    if not seg:
                        continue
                    if re.fullmatch(r'\r\n|\r|\n', seg):
                        # Newline inside an open tag → close the tag, emit newline, reopen it
                        if buffer_content.strip() == '':
                            # If the tag had no non-whitespace content so far, remove it
                            del result_tokens[last_open_index]
                        else:
                            result_tokens.append({'type': 'close'})
                        # Emit the newline as plain text
                        result_tokens.append({'type': 'text', 'text': seg})
                        # Reopen the same tag so that the newline is outside any tag
                        result_tokens.append({'type': 'open', 'tagname': current_open_tag})
                        last_open_index = len(result_tokens) - 1
                        buffer_content = ''
                    elif seg == '&':
                        # Ampersand inside an open tag → close the tag, emit &, reopen it
                        if buffer_content.strip() == '':
                            del result_tokens[last_open_index]
                        else:
                            result_tokens.append({'type': 'close'})
                        result_tokens.append({'type': 'text', 'text': '&'})
                        result_tokens.append({'type': 'open', 'tagname': current_open_tag})
                        last_open_index = len(result_tokens) - 1
                        buffer_content = ''
                    else:
                        # Regular text inside the tag
                        buffer_content += seg
                        result_tokens.append({'type': 'text', 'text': seg})
            else:
                # No tag is currently open → just emit the text
                result_tokens.append({'type': 'text', 'text': text})

    # 4) After processing all tokens, close any tag that’s still open (or remove if empty/whitespace-only)
    if current_open_tag is not None:
        if buffer_content.strip() == '':
            del result_tokens[last_open_index]
        else:
            result_tokens.append({'type': 'close'})
        current_open_tag = None
        last_open_index = None
        buffer_content = ''

    # 5) Reconstruct a single “raw” string from our token list
    raw_str = ''
    for tok in result_tokens:
        if tok['type'] == 'open':
            raw_str += f'<{tok["tagname"]}>'
        elif tok['type'] == 'close':
            raw_str += '</>'
        else:  # 'text'
            raw_str += tok['text']

    # 6) Replace any ampersand not directly adjacent to a newline with "and"
    raw_str = re.sub(r'(?<![\r\n])&(?!(?:[\r\n]))', 'and', raw_str)

    # 7) Remove any literal‐space run between </> and an opening‐tagged apostrophe (“<…>'”)
    raw_str = re.sub(r'(</>)[ ]+(<[^>]+>\')', r'\1\2', raw_str)

    # 8) Ensure there’s a space between </> and the next <RTP_…> tag if not preceded by '+'
    raw_str = re.sub(r'(?<!\+)</><(RTP_[^>]+)>', r'</> <\1>', raw_str)

    # 9) Ensure exactly one space between a digit or colon and the next opening tag,
    #    only when not immediately preceded by '+'
    raw_str = re.sub(r'(?<!\+)([0-9:])</>[ ]*<', r'\1</> <', raw_str)

    # 10) MERGE‐HYPHEN TAGS: If two consecutive tags share the same name and the second begins with ‘-’,
    #     merge them into one. For example:
    #         "<RTP_SkillPower>5</> <RTP_SkillPower>-minute cooldown</>"
    #         →
    #         "<RTP_SkillPower>5-minute cooldown</>"
    raw_str = re.sub(
        r'<(RTP_[^>]+)>([0-9]+)</>\s+<\1>-(.*?)</>',
        r'<\1>\2-\3</>',
        raw_str
    )

    # 11) MERGE MULTIPLE DIGITS: Repeatedly collapse runs of two <RTP_SkillTitleName> tags each
    #     wrapping a single digit into one. This loop repeats until no more merges are found.
    #
    #     E.g. "<RTP_SkillTitleName>1</> <RTP_SkillTitleName>2</>" → "<RTP_SkillTitleName>12</>"
    #          "<RTP_SkillTitleName>1</> <RTP_SkillTitleName>2</> <RTP_SkillTitleName>3</>"
    #            → (first pass merges 1&2) → "<RTP_SkillTitleName>12</> <RTP_SkillTitleName>3</>"
    #            → (second pass merges 12&3) → "<RTP_SkillTitleName>123</>"
    while True:
        merged = re.sub(
            r'<RTP_SkillTitleName>(\d)</>\s+<RTP_SkillTitleName>(\d)</>',
            r'<RTP_SkillTitleName>\1\2</>',
            raw_str
        )
        if merged == raw_str:
            break
        raw_str = merged

    # 12) Remove the space just added when the tag wraps a single letter (possibly with trailing punctuation).
    #     (e.g. “3</> <RTP_X>s</>” or “3</> <RTP_Default>s.</>” → “3</><RTP_Default>s.</>”)
    raw_str = re.sub(
        r'([0-9:])</> <([^>]+)>([A-Za-z])([%;,.:!\'"]?)</>',
        r'\1</><\2>\3\4</>',
        raw_str
    )

    # 13) Add a space between any letter and an opening <RTP_…> tag if missing
    raw_str = re.sub(r'([A-Za-z])<(RTP_[^>]+)>', r'\1 <\2>', raw_str)

    # 14) Remove any space immediately after an opening <RTP_…> tag
    raw_str = re.sub(r'(<RTP_[^>]+>) ', r'\1', raw_str)

    # 15) Remove any space immediately before a closing </> tag
    raw_str = re.sub(r' </>', r'</>', raw_str)

    # 16) Remove any space between </> and an opening‐tagged comma
    raw_str = re.sub(r'</>[ ]+<(RTP_[^>]+)>,', r'</><\1>,', raw_str)

    # 17) Generalized removal of spaces before punctuation‐in‐tags:
    #     semicolon, period, percent, apostrophe, comma, colon, exclamation mark.
    raw_str = re.sub(
        r'</>[ ]+<(RTP_[^>]+)>([%;,.:!\'"])</>',
        r'</><\1>\2</>',
        raw_str
    )

    # 18) Remove any space between </> and an opening‐tagged punctuation if
    #     that punctuation is the very first character inside the tag
    raw_str = re.sub(
        r'</>[ ]+<(RTP_[^>]+)>([:;,.!%\'"])',
        r'</><\1>\2',
        raw_str
    )

    # 19) Finally, collapse multiple spaces to one and remove spaces before punctuation, per line
    lines = raw_str.splitlines(keepends=True)
    processed_lines = []

    for line in lines:
        # Separate trailing newline (if any) so we don’t collapse it as a space
        if line.endswith('\r\n'):
            nl = '\r\n'
            content = line[:-2]
        elif line.endswith('\n') or line.endswith('\r'):
            nl = line[-1]
            content = line[:-1]
        else:
            nl = ''
            content = line

        # Collapse any run of 2+ spaces into a single space
        content = re.sub(r' {2,}', ' ', content)

        # Remove a space immediately before any punctuation: %, apostrophe, comma, semicolon, period, colon, exclamation mark
        content = re.sub(r" ([%,'\";.:!])", r"\1", content)

        processed_lines.append(content + nl)

    return ''.join(processed_lines)

def break_text_at_spaces(text_content: str, max_len: int) -> str:
    if not isinstance(text_content, str) or len(text_content) <= max_len:
        return text_content
    lines = []
    current_line_start_index = 0
    while current_line_start_index < len(text_content):
        if len(text_content) - current_line_start_index <= max_len:
            lines.append(text_content[current_line_start_index:])
            break
        search_end_exclusive = current_line_start_index + max_len
        possible_break_point = text_content.rfind(' ', current_line_start_index, search_end_exclusive +1) # search_end_exclusive should be inclusive for rfind

        if possible_break_point != -1 and possible_break_point > current_line_start_index :
            lines.append(text_content[current_line_start_index:possible_break_point])
            current_line_start_index = possible_break_point + 1 # Skip the space
        else: # No space found, or space is at the very beginning (should not happen with current_line_start_index > 0)
            # Hard break
            lines.append(text_content[current_line_start_index : current_line_start_index + max_len])
            current_line_start_index += max_len
    return '\n'.join(lines)

# --- NEW: Post-processing helper functions ---
def remove_accents(input_str: str) -> str:
    """
    Replaces all variants of Latin letters that have accents with the base letter.
    e.g., 'é' -> 'e', 'ā' -> 'a'. This uses Unicode normalization.
    """
    if not isinstance(input_str, str):
        return input_str
    # Normalize to NFD (Canonical Decomposition Form).
    # This separates base characters from their combining marks.
    nfkd_form = unicodedata.normalize('NFD', input_str)
    # Filter out the combining marks (those in the 'Mn' category - Mark, Nonspacing).
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

def replace_escaped_quotes_with_smart_quotes(text: str) -> str:
    if not isinstance(text, str) or '\"' not in text:
        return text

    # Regex to split by tags, keeping the tags. '<.*?>' matches any tag non-greedily.
    tag_pattern = r'(<.*?>)'
    parts = re.split(tag_pattern, text)

    final_output = []
    use_opening_quote = True  # Global state for alternating quotes

    for part in parts:
        if not part:  # Skip empty strings that can result from re.split
            continue

        # Check if the part is a tag (heuristic: starts with < and ends with > and was a delimiter)
        # A more robust check for parts generated by re.split(r'(capture_group)', text) is to see if it was one of the captured delimiters.
        # However, re.split interleaves non-matched text with matched delimiters.
        # A simple check: if the global text contained the part as a tag.
        # For simplicity here, we'll assume parts that look like tags are tags.
        # A part is a tag if it was captured by the regex.
        # re.split with a capturing group returns [non_match, match, non_match, match, ...].
        # We can identify tags by their structure or by checking if they match the pattern.
        # A simpler way: if `re.fullmatch(tag_pattern, part)` is true, it's a tag.

        is_a_tag = bool(re.fullmatch(tag_pattern, part)) # Check if the part itself is a complete tag

        if is_a_tag:
            final_output.append(part)
        else:
            # This is a text segment, apply original smart quote logic
            segment_sub_parts = part.split('\"')
            for i, sub_part in enumerate(segment_sub_parts):
                final_output.append(sub_part)
                if i < len(segment_sub_parts) - 1:  # A quote was here
                    if use_opening_quote:
                        final_output.append("“")
                    else:
                        final_output.append("”")
                    use_opening_quote = not use_opening_quote
    return "".join(final_output)


def replace_chinese_bullet_with_hyphen(text: str) -> str:
    if not isinstance(text, str) or '·' not in text:
        return text
    # Pad bullets with spaces, then replace, then clean up.
    # This ensures "a·b" -> "a - b" and "a · b" -> "a - b"
    # and allows for leading/trailing spaces if the bullet is at an edge.
    processed_text = text.replace('·', ' - ')
    # Collapse multiple spaces into one.
    processed_text = re.sub(r' +', ' ', processed_text)
    return processed_text

def fix_possessive_s(text: str) -> str:
    if not isinstance(text, str):
        return text
    return text.replace("s's", "s'")


# --- START: Map Name Acronym Logic ---
def create_map_name_acronym(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        return ""

    # Special replacements, checked case-insensitively
    special_replacements = {
        "hehuan": "HH",
        "guiwang": "GW",
        "qingyun": "QY",
        "lingxi": "LX",
        "fenxiang": "FX",
        "heyang": "HY"
    }

    words = name.split(' ')
    acronym_parts = []

    for word in words:
        if not word:
            continue
        # Check for special replacements
        lower_word = word.lower()
        if lower_word in special_replacements:
            acronym_parts.append(special_replacements[lower_word])
        # Otherwise, take the first letter
        else:
            acronym_parts.append(word[0].upper())

    return "".join(acronym_parts)
# --- END: Map Name Acronym Logic ---

# --- Main Processing ---
def main():
    print(f"Starting patch generation process...")
    if INSERT_DEBUG_IDS:
        print("  *** DEBUG MODE ENABLED: Namespace and Key will be prepended to translated strings. ***")
    print(f"  Input translations: {TRANSLATED_UNIFIED_JSON_PATH}")
    print(f"  Input LocRes structure (CSV): {UNIFIED_LOCRES_HASH_CSV_PATH}")
    print(f"  Input Key Source Origins: {KEY_SOURCE_ORIGINS_JSON_PATH}")
    print(f"  Output base directory: {PATCH_BASE_OUTPUT_DIR}")
    print(f"  Post-processed debug output: {POSTPROCESSED_TRANSLATIONS_DEBUG_JSON_PATH}")

    # 1. Load translations
    try:
        with open(TRANSLATED_UNIFIED_JSON_PATH, 'r', encoding='utf-8') as f:
            raw_translated_json_content = json.load(f)
        translated_json_content = {}
        for ns, key_values_dict in raw_translated_json_content.items():
            cleaned_ns = clean_key_bom(ns)
            translated_json_content[cleaned_ns] = {clean_key_bom(k): v for k, v in key_values_dict.items()}
        print(f"  Successfully loaded and pre-processed translations.")
    except FileNotFoundError:
        print(f"ERROR: Translated JSON file not found: {TRANSLATED_UNIFIED_JSON_PATH}"); return
    except Exception as e:
        print(f"ERROR: Could not read or pre-process translated JSON '{TRANSLATED_UNIFIED_JSON_PATH}': {e}"); return

    # --- Apply post-processing to translated content (in-memory) ---
    print(f"\n--- Applying initial post-processing to translated content ---")

    POSTPROC_NAMESPACE_CONFIG = {
        "FZCTmplTaskTalk": {"replace_spaces_with_mid_space": True},
        "RareEquipmentShop": {"line_break_max": 10},
        "ActivityShop": {"line_break_max": 10},
        "CampBlueShop": {"line_break_max": 10},
        "CampRedShop": {"line_break_max": 10},
        "DailyRewardShop": {"line_break_max": 10},
        "FashionShop": {"line_break_max": 10},
        "HYCPTShop": {"line_break_max": 10},
        "NvwaStoneShop": {"line_break_max": 10},
        "ShiTuShop": {"line_break_max": 10},
        "SuitShop": {"line_break_max": 10},
        "faction_shop": {"line_break_max": 10},
        #"escape_character_class_with_figure": {"line_break_max": 180},
        #"escape_character_class": {"line_break_max": 180},
        #"speakdata": {"line_break_max": 999},
        "mapdata": {"line_break_max": 60}
    }
    RTP_PROCESSING_COUNT = 0
    LINE_BREAK_COUNT = 0
    SMART_QUOTE_COUNT = 0
    BULLET_REPLACE_COUNT = 0
    POSSESSIVE_FIX_COUNT = 0
    COLON_SPACE_ADD_COUNT = 0
    ACTIVITY_NAME_SHORTEN_COUNT = 0
    ACCENT_REMOVAL_COUNT = 0
    MID_SPACE_REPLACE_COUNT = 0 # NEW counter


    for ns, texts_in_ns in translated_json_content.items():
        # --- NEW: Special handling for LimitedTimeActivityConfig ---
        if ns == "LimitedTimeActivityConfig":
            # Create a copy of items to iterate over, as we are modifying the dictionary
            for key, text_value in list(texts_in_ns.items()):
                if isinstance(text_value, str) and 0 < len(text_value) < 19:
                    original_value = text_value
                    new_value = ""
                    # Check for multiple words (acronym target)
                    if ' ' in text_value.strip():
                        words = text_value.split()
                        # Filter out empty strings that might result from multiple spaces
                        acronym = "".join(word[0].upper() for word in words if word)
                        new_value = acronym
                    # Handle single word (truncation target)
                    else:
                        new_value = text_value[:5] + "."

                    # Update if the value has changed
                    if new_value != original_value:
                        texts_in_ns[key] = new_value
                        ACTIVITY_NAME_SHORTEN_COUNT += 1

        ns_config = POSTPROC_NAMESPACE_CONFIG.get(ns, {})
        line_break_max_len_for_ns = ns_config.get("line_break_max")

        for key, text_value in texts_in_ns.items():
            if isinstance(text_value, str) and text_value.strip():
                processed_text = text_value

                # --- START: DEBUG ID INJECTION ---
                if INSERT_DEBUG_IDS:
                    # Prepends "Namespace_ID " to the string for easy identification in-game.
                    # This happens before any other text processing (like line breaking)
                    # to ensure subsequent steps account for the new string length.
                    debug_prefix = f"{ns}_{key} "
                    processed_text = debug_prefix + processed_text
                # --- END: DEBUG ID INJECTION ---

                # --- NEW STEP: Remove Accents ---
                # This should be one of the first steps to ensure other logic (e.g. line breaks)
                # operates on the final, clean characters.
                text_before_accent_removal = processed_text
                processed_text = remove_accents(processed_text)
                if processed_text != text_before_accent_removal:
                    ACCENT_REMOVAL_COUNT += 1
                # --- END: NEW STEP ---

                # 1. Apply line breaking if namespace is configured for it
                if line_break_max_len_for_ns is not None:
                    text_before_line_break = processed_text
                    processed_text = break_text_at_spaces(processed_text, line_break_max_len_for_ns)
                    if processed_text != text_before_line_break:
                        LINE_BREAK_COUNT +=1

                # 2. Replace escaped quotes with smart quotes
                text_before_smart_quotes = processed_text
                if '\"' in processed_text:
                    processed_text = replace_escaped_quotes_with_smart_quotes(processed_text)
                    if processed_text != text_before_smart_quotes:
                        SMART_QUOTE_COUNT += 1

                # 3. Replace Chinese bullet points with hyphen (updated logic)
                text_before_bullet_replace = processed_text
                if '·' in processed_text:
                    processed_text = replace_chinese_bullet_with_hyphen(processed_text)
                    if processed_text != text_before_bullet_replace:
                        BULLET_REPLACE_COUNT += 1

                # 4. Fix possessive s's -> s'
                text_before_possessive_fix = processed_text
                if "s's" in processed_text:
                    processed_text = fix_possessive_s(processed_text)
                    if processed_text != text_before_possessive_fix:
                        POSSESSIVE_FIX_COUNT += 1

                # 5. Apply RTP correction (includes newline migration and spacing)
                if ("RTP" in processed_text or "<Def>" in processed_text) and ns != "WildCardHandlers": # Added common triggers for RTP handler
                    text_before_rtp = processed_text
                    processed_text = handle_rtp_tags(processed_text)
                    if processed_text != text_before_rtp:
                            RTP_PROCESSING_COUNT +=1
                else:
                    # Add space after colon for non-RTP strings
                    # Also handle if it ends with colon then closing tag (e.g. ":</>")
                    original_processed_text_for_colon_check = processed_text
                    if isinstance(processed_text, str):
                        if processed_text.endswith(':'):
                            processed_text += " "
                        elif processed_text.endswith(':</>'): # This specific case for non-RTP strings is preserved
                            processed_text = processed_text[:-3] + " </>"

                        if processed_text != original_processed_text_for_colon_check:
                            COLON_SPACE_ADD_COUNT +=1

                # --- NEW STEP: Replace spaces with mid-space for specific namespaces ---
                if ns_config.get("replace_spaces_with_mid_space", False):
                    text_before_mid_space = processed_text
                    # Replace standard space (U+0020) with a narrower space (U+2005 FOUR-PER-EM SPACE)
                    # to help with line length issues in UI designed for Chinese.
                    processed_text = processed_text.replace(' ', ' ')
                    if processed_text != text_before_mid_space:
                        MID_SPACE_REPLACE_COUNT += 1
                # --- END: NEW STEP ---

                texts_in_ns[key] = processed_text
            elif isinstance(text_value, str) and not text_value.strip() and key == "":
                texts_in_ns[key] = ""

    if ACTIVITY_NAME_SHORTEN_COUNT > 0:
        print(f"  Shortened {ACTIVITY_NAME_SHORTEN_COUNT} entries in 'LimitedTimeActivityConfig'.")
    if ACCENT_REMOVAL_COUNT > 0:
        print(f"  Removed accents from {ACCENT_REMOVAL_COUNT} entries.")
    if RTP_PROCESSING_COUNT > 0:
        print(f"  Applied RTP tag processing to {RTP_PROCESSING_COUNT} entries.")
    if LINE_BREAK_COUNT > 0:
        print(f"  Applied line breaking to {LINE_BREAK_COUNT} entries in targeted namespaces.")
    if SMART_QUOTE_COUNT > 0:
        print(f"  Replaced escaped quotes with smart quotes in {SMART_QUOTE_COUNT} entries.")
    if BULLET_REPLACE_COUNT > 0:
        print(f"  Replaced Chinese bullet points with hyphens in {BULLET_REPLACE_COUNT} entries.")
    if POSSESSIVE_FIX_COUNT > 0:
        print(f"  Corrected 's's to s' in {POSSESSIVE_FIX_COUNT} entries.")
    if COLON_SPACE_ADD_COUNT > 0:
        print(f"  Added space after colon (or before closing tag after colon) for {COLON_SPACE_ADD_COUNT} non-RTP entries.")
    if MID_SPACE_REPLACE_COUNT > 0: # NEW log
        print(f"  Replaced standard spaces with mid-spaces in {MID_SPACE_REPLACE_COUNT} entries for targeted namespaces.")
    print(f"--- Initial post-processing complete ---")

    # --- START: New Buff ID Insertion Logic ---
    print(f"\n--- Inserting Buff IDs into ZCTooltipBuffDoc ---")
    buff_id_insert_count = 0
    tooltip_ns = "ZCTooltipBuffDoc"
    if tooltip_ns in translated_json_content:
        # Iterate over a copy of the items to allow modification during the loop
        for key, text_value in list(translated_json_content[tooltip_ns].items()):
            # Check if the key matches the pattern "ID-TipBuffEffect" and the value is a string
            if isinstance(key, str) and key.endswith("-TipBuffEffect") and isinstance(text_value, str):
                # Extract the ID part of the key
                id_part = key.split('-')[0]
                # Ensure the extracted part is a digit before proceeding
                if id_part.isdigit():
                    original_value = text_value
                    # Append the ID to the effect description
                    new_value = f"{original_value}\nID: {id_part}"
                    # Update the dictionary with the new value
                    translated_json_content[tooltip_ns][key] = new_value
                    buff_id_insert_count += 1

    if buff_id_insert_count > 0:
        print(f"  Inserted {buff_id_insert_count} buff IDs into '{tooltip_ns}' effect descriptions.")
    print(f"--- Buff ID insertion complete ---")
    # --- END: New Buff ID Insertion Logic ---


    # --- START: New Map Name Acronym Generation Step ---
    print(f"\n--- Applying Map Name Acronym generation ---")
    map_name_acronym_count = 0
    map_name_ns = "MapEditorMapName"
    if map_name_ns in translated_json_content:
        for key, text_value in translated_json_content[map_name_ns].items():
            if isinstance(text_value, str) and text_value:
                acronym = create_map_name_acronym(text_value)
                if acronym != text_value:
                    translated_json_content[map_name_ns][key] = acronym
                    map_name_acronym_count += 1
    if map_name_acronym_count > 0:
        print(f"  Converted {map_name_acronym_count} entries in '{map_name_ns}' to acronyms.")
    print(f"--- Map Name Acronym generation complete ---")
    # --- END: New Map Name Acronym Generation Step ---


    # --- START: New Final Post-processing Step for Deletions and Direct Overrides ---
    print(f"\n--- Applying final overrides and deletions ---")

    KEYS_TO_DELETE = [

    ]


    DIRECT_OVERRIDES = {
        "": {
            # Fixes for J window (vertical text!)
            "165069BD4B390D739B401B8230D776DD": "SWAN",
            "F67F3404443FBC8B145982A1B1A295DC": "GOOSE",
            "84F2BF404CE76A44CB0677BBB144F463": "INFINITE",
            "9494C8464BD4027273B9A1A3A8C08906": " VISTAS",
            "FAB2F974485FFC807CAC7288C4B53BBE": " PAINTINGS",
            "C09F2E0E4C5B56F0FF21838520168BB0": "LEISURE",
            "3785CE6247463815101646B447D2154A": " COLLECT",
            "020A747547E58132E12ADBA8C382651C": "EVERTYTHING",
            "999E50B94E44EE3F6320AEA559AA3B4B": " SEARCH",
            "5D09D14A482EC30436C7668B9C0AB93D": "CLOUDSTEP",
            "D51F6A344B9116FC35724F80C3591E1A": " MORTAL",
            "9AE7B77B40BFB0072A6C22B310F05FE7": "AFFAIRS",
            "0B52397E4BE6F2886236DE8F69E010C8": "ANECDOTES",
            "085CF12F4C6F8975AE5754A95CA97316": " CODEX",
            "F5B2CED24807F7A657A6899580EF3612": "⚡",
            "C0392C3748C354B48F4EF6AFDDDB09F8": "👑",
            "CE205DBA481DF69A88838CB9D6E10B03": "🏛️",
            "EF8ECDA447CA4774F37DC7AEA0F314FD": "",
            "50ECCE9640BA5A56F170ACB7D66AED01": "Lvl."
        },
        # Revert this or guild menu breaks.
        # Todo, test if we can bring some back. It does introduce Chinese!
        "OccupationCommon": {
            # "3": "全部門派",
            # "5": "全部陣營",
            # "4": "無",
            #"1": "無限制",
            "1": "无限制",
            # "2": "未知職業",
            # "7": "逍遙",
            # "6": " 中立",
            # "8": " 天衡"
        },
        # "faction_limit": {
        #     "148470-name": "合歡",
        #     "148471-name": "靈汐",
        #     "148472-name": "焚香",
        #     "148468-name": "青雲",
        #     "148469-name": "鬼王"
        # },
        "occupation_id2name": {
            "3394-occupation_name": "HH",
            "4732-occupation_name": "None",
            #"1348-occupation_name": "無限制",
            "1348-occupation_name": "无限制",
            "1351-occupation_name": "LX",
            "3395-occupation_name": "FX",
            "1349-occupation_name": "QY",
            "1350-occupation_name": "GW"
        },
        "BaoGuoItem": {
            "3": "万",      # Original Chinese value
            "4": "亿",      # Original Chinese value
            "8": "b",        # Original value from Chinese locres (English char)
            "6": "k",        # Original value from Chinese locres (English char)
            "7": "m"         # Original value from Chinese locres (English char)
        },
        "Base": {
            "1": "一",      # Original Chinese value
            "7": "七",      # Original Chinese value
            "13": "万",     # Original Chinese value
            "3": "三",      # Original Chinese value
            "9": "九",      # Original Chinese value
            "2": "二",      # Original Chinese value
            "5": "五",      # Original Chinese value
            "14": "亿",     # Original Chinese value
            "15": "兆",     # Original Chinese value
            "8": "八",      # Original Chinese value
            "6": "六",      # Original Chinese value
            "10": "十",     # Original Chinese value
            "12": "千",     # Original Chinese value
            "4": "四",      # Original Chinese value
            "11": "百",     # Original Chinese value
            "0": "零",       # Original Chinese value
            "20": "Ten Billion",
            "21": "One Hundred Billion",
            "16": "One Hundred Thousand",
            "17": "One Million",
            "18": "Ten Million",
            "19": "One Billion"
        }
    }

    override_count = 0
    delete_count = 0

    # Apply Direct Overrides First
    for ns_override, keys_values_override in DIRECT_OVERRIDES.items():
        # Ensure the namespace exists in translated_json_content or create it
        # Using setdefault ensures that if the namespace (e.g. "") exists, we use it, otherwise it's created.
        ns_dict = translated_json_content.setdefault(ns_override, {})
        for key_override, value_override in keys_values_override.items():
            ns_dict[key_override] = value_override # Set/overwrite the value
            override_count += 1

    if override_count > 0:
        print(f"  Applied direct overrides to {override_count} specified entries.")

    # Apply Deletions
    for ns_to_delete, key_to_delete in KEYS_TO_DELETE:
        if ns_to_delete in translated_json_content and key_to_delete in translated_json_content[ns_to_delete]:
            del translated_json_content[ns_to_delete][key_to_delete]
            delete_count += 1
            # print(f"  Deleted key '{key_to_delete}' from namespace '{ns_to_delete}'.") # Can be verbose
            if not translated_json_content[ns_to_delete]: # Check if namespace became empty
                del translated_json_content[ns_to_delete]
                # print(f"  Removed empty namespace '{ns_to_delete}' after deletion.")
        else:
            print(f"  WARNING: Key '{key_to_delete}' in namespace '{ns_to_delete}' not found for deletion during override step.")

    if delete_count > 0:
        print(f"  Deleted {delete_count} specified entries.")
    print(f"--- Final overrides and deletions complete ---")
    # --- END: New Final Post-processing Step ---


    print(f"\n--- Exporting post-processed translations to {POSTPROCESSED_TRANSLATIONS_DEBUG_JSON_PATH} ---")
    try:
        with open(POSTPROCESSED_TRANSLATIONS_DEBUG_JSON_PATH, 'w', encoding='utf-8') as f_debug:
            json.dump(translated_json_content, f_debug, ensure_ascii=False, indent=4)
        print(f"  Successfully exported post-processed translations for debugging.")
    except Exception as e:
        print(f"  ERROR: Could not export post-processed translations: {e}")

    locres_csv_data_map, namespace_info_from_csv = load_data_with_hashes_from_csv(UNIFIED_LOCRES_HASH_CSV_PATH)
    if locres_csv_data_map is None:
        print("ERROR: Failed to load LocRes data map from CSV. Exiting."); return

    try:
        with open(KEY_SOURCE_ORIGINS_JSON_PATH, 'r', encoding='utf-8') as f:
            key_source_origins_data = json.load(f)
        print(f"  Successfully loaded key source origins from '{KEY_SOURCE_ORIGINS_JSON_PATH}'.")
    except FileNotFoundError:
        print(f"ERROR: Key Source Origins JSON file not found: {KEY_SOURCE_ORIGINS_JSON_PATH}"); return
    except Exception as e:
        print(f"ERROR: Could not read or parse Key Source Origins JSON '{KEY_SOURCE_ORIGINS_JSON_PATH}': {e}"); return

    temp_locres_data_map = defaultdict(lambda: {"namespace_name": None, "namespace_hash": 0, "entries": []})
    formatstring_txt_data = defaultdict(dict)
    formatstring_json_data = defaultdict(lambda: defaultdict(dict))

    locres_entries_count = 0
    formatstring_txt_entries_count = 0
    formatstring_json_entries_count = 0
    ui_assets_to_txt_count = 0 # REVISED: Counter for UI keys that are now saved
    keys_not_found_in_translations = 0


    print(f"\n--- Preparing data for LocRes and FormatString files based on key_source_origins ---")
    for ns, keys_map in key_source_origins_data.items():
        for key, original_sources_details_list in keys_map.items():
            # Retrieve translation from the (potentially overridden/deleted) translated_json_content
            # If ns or key was deleted, .get() will handle it gracefully.
            translation = translated_json_content.get(ns, {}).get(key)

            if translation is None:
                # Key was deleted or never existed in translated_json_content. Skip processing for outputs.
                # Only log if it was expected from key_source_origins but not found in final translations.
                # Check if it was *supposed* to be there (i.e., not one of the explicitly deleted keys)
                is_explicitly_deleted = False
                for del_ns, del_key in KEYS_TO_DELETE: # Check against the deletion list
                    if ns == del_ns and key == del_key:
                        is_explicitly_deleted = True
                        break
                if not is_explicitly_deleted:
                        keys_not_found_in_translations +=1
                        # print(f"  INFO: Key '{key}' in namespace '{ns}' from key_source_origins not found in final translations (possibly deleted or missing). Skipping for output generation.")
                continue # Skip this key entirely for output generation

            value_for_patch = translation # Use the final processed/overridden translation

            is_locres_capable = (ns, key) in locres_csv_data_map
            processed_for_locres_this_key = False
            processed_for_fstxt_this_key = False
            processed_for_fsjson_this_key = False
            source_types_for_this_key = set()
            for source_detail_check in original_sources_details_list:
                source_types_for_this_key.add(source_detail_check if isinstance(source_detail_check, str) else source_detail_check.get("type"))

            for source_detail in original_sources_details_list:
                source_type = None
                current_metadata_for_json = None
                if isinstance(source_detail, str):
                    source_type = source_detail
                elif isinstance(source_detail, dict) and "type" in source_detail:
                    source_type = source_detail["type"]
                    if source_type == FS_JSON_TYPE:
                        current_metadata_for_json = source_detail.get("metadata", {"flags": "", "note": ""})
                else:
                    print(f"WARNING: Unknown source detail format for {ns}/{key}: {source_detail}. Skipping this source detail.")
                    continue

                if source_type == LOCRES_TYPE and is_locres_capable and not processed_for_locres_this_key:
                    csv_entry_details = locres_csv_data_map[(ns, key)]
                    if temp_locres_data_map[ns]["namespace_name"] is None:
                        temp_locres_data_map[ns]["namespace_name"] = ns
                        temp_locres_data_map[ns]["namespace_hash"] = csv_entry_details["namespace_hash"]
                    locres_value = normalize_line_endings(str(value_for_patch))
                    temp_locres_data_map[ns]["entries"].append({
                        "key_string": key, "key_hash": csv_entry_details["key_hash"],
                        "translated_value": locres_value,
                        "source_string_hash": csv_entry_details["source_string_hash"]
                    })
                    locres_entries_count += 1
                    processed_for_locres_this_key = True
                elif source_type == FS_TXT_TYPE and not processed_for_fstxt_this_key:
                    formatstring_txt_data[ns][key] = value_for_patch
                    formatstring_txt_entries_count += 1
                    processed_for_fstxt_this_key = True
                elif source_type == FS_JSON_TYPE and not processed_for_fsjson_this_key:
                    if current_metadata_for_json is None:
                        current_metadata_for_json = {"flags": "", "note": ""}
                    formatstring_json_data[ns][key] = {"text": value_for_patch, "metadata": current_metadata_for_json}
                    formatstring_json_entries_count += 1
                    processed_for_fsjson_this_key = True

            is_only_ui = True
            has_other_valid_fs_type = False
            for st in source_types_for_this_key:
                if st != UI_ASSETS_TYPE: is_only_ui = False
                if st == FS_TXT_TYPE or st == FS_JSON_TYPE: has_other_valid_fs_type = True

            # REVISED LOGIC: Instead of discarding UI-only keys, save them to a special .txt file
            if is_only_ui and not processed_for_locres_this_key and not has_other_valid_fs_type:
                # This key is only from a UI Asset and cannot be put into locres.
                # Save it to our special placeholder namespace to be written to a dedicated file.
                formatstring_txt_data[UI_ASSETS_PLACEHOLDER_NS][key] = value_for_patch
                ui_assets_to_txt_count += 1

    if keys_not_found_in_translations > 0:
        print(f"  INFO: {keys_not_found_in_translations} keys from key_source_origins were not found in the final translation data (and were not in the explicit delete list).")


    final_locres_data_for_generation_list = []
    if namespace_info_from_csv:
        sorted_locres_ns_from_csv_order = sorted(namespace_info_from_csv.keys(), key=lambda k_ns: namespace_info_from_csv[k_ns]["order"])
        for ns_name_csv_ordered in sorted_locres_ns_from_csv_order:
            if ns_name_csv_ordered in temp_locres_data_map and temp_locres_data_map[ns_name_csv_ordered]["entries"]:
                final_locres_data_for_generation_list.append(temp_locres_data_map[ns_name_csv_ordered])
        for ns_name_map_key in temp_locres_data_map:
            if ns_name_map_key not in namespace_info_from_csv and temp_locres_data_map[ns_name_map_key]["entries"]:
                print(f"WARNING: Namespace '{ns_name_map_key}' in LocRes data but not in initial CSV namespace order. Appending.")
                final_locres_data_for_generation_list.append(temp_locres_data_map[ns_name_map_key])
    else:
        for ns_name_map_key in sorted(temp_locres_data_map.keys()):
            if temp_locres_data_map[ns_name_map_key]["entries"]:
                final_locres_data_for_generation_list.append(temp_locres_data_map[ns_name_map_key])


    actual_locres_entries = sum(len(data["entries"]) for data in final_locres_data_for_generation_list)
    actual_formatstring_txt_entries = sum(len(v) for v in formatstring_txt_data.values())
    actual_formatstring_json_entries = sum(len(v) for v in formatstring_json_data.values())

    if actual_locres_entries > 0: print(f"  Prepared {actual_locres_entries} entries across {len(final_locres_data_for_generation_list)} namespaces for .locres generation.")
    else: print(f"  No data prepared for .locres generation.")
    if actual_formatstring_txt_entries > 0: print(f"  Prepared {actual_formatstring_txt_entries} entries for FormatString .txt generation.")
    else: print(f"  No data prepared for FormatString .txt generation.")
    if actual_formatstring_json_entries > 0: print(f"  Prepared {actual_formatstring_json_entries} entries for FormatString .json generation.")
    else: print(f"  No data prepared for FormatString .json generation.")
    if ui_assets_to_txt_count > 0: print(f"  Saved {ui_assets_to_txt_count} UI-only keys (not locres-capable) to be written to '{UI_ASSETS_FILENAME}'.")

    print(f"\n--- Ensuring clean output directories ---")
    if os.path.exists(LOCRES_OUTPUT_BASE): shutil.rmtree(LOCRES_OUTPUT_BASE)
    if os.path.exists(TEXT_FILES_OUTPUT_BASE): shutil.rmtree(TEXT_FILES_OUTPUT_BASE)
    os.makedirs(LOCRES_OUTPUT_BASE, exist_ok=True)
    os.makedirs(TEXT_FILES_OUTPUT_BASE, exist_ok=True)
    locres_dir_zh_hans = os.path.join(LOCRES_OUTPUT_BASE, "zh-Hans")
    locres_dir_zh_hant = os.path.join(LOCRES_OUTPUT_BASE, "zh-Hant")
    os.makedirs(locres_dir_zh_hans, exist_ok=True)
    os.makedirs(locres_dir_zh_hant, exist_ok=True)
    print(f"--- Output directories prepared ---")

    if final_locres_data_for_generation_list:
        locres_path_hans = os.path.join(locres_dir_zh_hans, "Game.locres")
        generate_locres_file_v3_with_hashes(final_locres_data_for_generation_list, locres_path_hans)
        if os.path.exists(locres_path_hans):
            locres_path_hant = os.path.join(locres_dir_zh_hant, "Game.locres")
            try:
                shutil.copy2(locres_path_hans, locres_path_hant)
                print(f"    Successfully copied Game.locres to {locres_path_hant}")
            except Exception as e:
                print(f"    ERROR: Could not copy Game.locres to {locres_path_hant}: {e}")

            # --- START: New copy locations for Game.locres ---
            print(f"    Copying Game.locres to ZCTranslateData directories...")
            new_target_copy_relative_paths = [
                os.path.join("ZhuxianClient", "gamedata", "client", "ZCTranslateData", "Game", "en"),
                os.path.join("ZhuxianClient", "gamedata", "client", "ZCTranslateData", "Game", "ru"),
                os.path.join("ZhuxianClient", "gamedata", "client", "ZCTranslateData", "Game", "zh-Hans"),
                os.path.join("ZhuxianClient", "gamedata", "client", "ZCTranslateData", "Game", "zh-Hant"),
            ]

            for relative_dir in new_target_copy_relative_paths:
                target_full_dir = os.path.join(PATCH_BASE_OUTPUT_DIR, relative_dir)
                os.makedirs(target_full_dir, exist_ok=True)
                target_locres_path = os.path.join(target_full_dir, "Game.locres")
                try:
                    shutil.copy2(locres_path_hans, target_locres_path)
                    print(f"      Successfully copied Game.locres to {target_locres_path}")
                except Exception as e:
                    print(f"      ERROR: Could not copy Game.locres to {target_locres_path}: {e}")
            # --- END: New copy locations for Game.locres ---
        else:
            print(f"    Skipping copies as source {locres_path_hans} was not generated.")
    else:
        print("\nNo data available for .locres generation.")

    generate_format_string_txt_files(formatstring_txt_data, TEXT_FILES_OUTPUT_BASE)
    generate_format_json_files(formatstring_json_data, TEXT_FILES_OUTPUT_BASE)

    print("\n--- Patch file generation process completed. ---")

if __name__ == "__main__":
    critical_input_missing = False
    if not os.path.exists(KEY_SOURCE_ORIGINS_JSON_PATH):
        print(f"ERROR: CRITICAL INPUT MISSING - Key Source Origins JSON file: '{KEY_SOURCE_ORIGINS_JSON_PATH}'.")
        critical_input_missing = True
    if not os.path.exists(TRANSLATED_UNIFIED_JSON_PATH):
        print(f"ERROR: CRITICAL INPUT MISSING - Translated JSON file: '{TRANSLATED_UNIFIED_JSON_PATH}'.")
        critical_input_missing = True
    if not os.path.exists(UNIFIED_LOCRES_HASH_CSV_PATH):
        print(f"ERROR: CRITICAL INPUT MISSING - Unified LocRes Hash CSV: '{UNIFIED_LOCRES_HASH_CSV_PATH}'.")
        critical_input_missing = True

    if not critical_input_missing:
        main()
    else:
        print("Exiting due to missing critical input files.")