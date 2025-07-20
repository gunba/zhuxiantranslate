import os
import json
import csv
import subprocess
import shutil
import tempfile
# import glob
from collections import defaultdict
import zlib

# --- CityHash Import and Functions ---
try:
    import cityhash
except ImportError:
    print("ERROR: Python library 'cityhash' not found.")
    print("Please install it by running: pip install cityhash")
    cityhash = None

def normalize_line_endings_for_hash(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return text.replace('\r\n', '\n').replace('\r', '\n').replace('\n', '\r\n')

def calculate_optimized_cityhash64_utf16_key_hash(key_string: str) -> int:
    if not cityhash:
        raise ImportError("cityhash library is required for this function but is not available.")
    if not isinstance(key_string, str):
        raise TypeError("Input 'key_string' must be a string.")

    normalized_key = normalize_line_endings_for_hash(key_string)
    encoded_key_bytes = normalized_key.encode('utf-16-le')
    h64 = cityhash.CityHash64(encoded_key_bytes)
    low32 = h64 & 0xFFFFFFFF
    high32 = (h64 >> 32) & 0xFFFFFFFF
    final_hash = (low32 + (high32 * 23)) & 0xFFFFFFFF
    return final_hash

def calculate_source_string_hash(text: str) -> int:
    if not isinstance(text, str):
        raise TypeError("Input must be a string.")
    encoded = text.encode('utf-16-le') + b'\x00\x00'
    return zlib.crc32(encoded) & 0xFFFFFFFF

# --- Configuration ---
EXTRACTED_DATA_DIR = "C:/Users/jorda/PycharmProjects/zxsjlocpipe/zxsj_output"
UNREAL_LOCRES_EXE_PATH = "./UnrealLocres.exe"
FINAL_JSON_OUTPUT_PATH = "./unified_zxsj_data.json"
UNIFIED_LOCRES_HASH_CSV_PATH = "./unified_locres_with_hashes.csv"
KEY_SOURCE_ORIGINS_JSON_PATH = "./key_source_origins.json"

#GAME_VERSIONS_ORDER = [
#    "ZXSJ_Speed",
#    "ZXSJ_Speed_CN",
#    "ZXSJ_Speed_TW",
#    "ZXSJ_Speed_RU",
#    "ZXSJ_Speed_EN",
#    "ZXSJ_Speed_S1",
#    "zxsjgt",
#    "zxsjgt_RU",
#    "zxsjgt_CN",
#    "zxsjgt_TW",
#    "zxsjgt_EN"
#]

GAME_VERSIONS_ORDER = ["zxsjgt", "ZXSJ_Speed", "zxsjgt_RU", "zxsjgt_CN", "zxsjgt_TW", "zxsjgt_EN"]
GAME_VERSIONS_ORDER.reverse()

SOURCE_CONFIG = {
    "FormatString_Txt": {
        "pak_group": "ClientGameData",
        "subfolder": "FormatString",
        "priority": 1,
        "handler": "parse_formatstring_txt"
    },
    "FormatString_Json": {
        "pak_group": "ClientGameData",
        "subfolder": "FormatString",
        "priority": 2,
        "handler": "parse_formatstring_json"
    },
    "LocRes": {
        "pak_group": "pakchunk0",
        "priority": 3,
        "handler": "process_locres_folder"
    },
    "UI_Assets": {
        "pak_group": "pakchunk16_UI_JSON",
        "subfolder": "UI",
        "priority": 4,
        "handler": "process_ui_assets_folder"
    }
}

# --- Parsing Helper Functions ---

def run_unreal_locres(locres_file_path, output_csv_path):
    if not os.path.exists(UNREAL_LOCRES_EXE_PATH):
        print(f"    ERROR: UnrealLocres.exe not found at {UNREAL_LOCRES_EXE_PATH}")
        return False
    if not os.path.exists(locres_file_path):
        # This can be normal if a pak just doesn't contain a Game.locres
        # print(f"    INFO: Locres file not found: {locres_file_path}")
        return False
    cmd = [UNREAL_LOCRES_EXE_PATH, "export", locres_file_path, "-f", "csv", "-o", output_csv_path]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, encoding='utf-8', errors='replace')
        if result.stderr and ("error" in result.stderr.lower() or "failed" in result.stderr.lower()):
            print(f"    UnrealLocres STDERR: {result.stderr.strip()}")
        return os.path.exists(output_csv_path)
    except subprocess.CalledProcessError as e:
        print(f"    Error running UnrealLocres.exe for {locres_file_path}. Return code: {e.returncode}")
        if e.stdout: print(f"    STDOUT: {e.stdout}")
        if e.stderr: print(f"    STDERR: {e.stderr}")
        return False
    except Exception as e:
        print(f"    Error running UnrealLocres.exe for {locres_file_path}: {e}")
        return False

def parse_locres_csv(csv_path):
    data = defaultdict(lambda: defaultdict(dict))
    contributed_keys = 0
    try:
        with open(csv_path, mode='r', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            expected_headers = ['key', 'source', 'namespacehash', 'keyhash', 'sourcestringhash']
            reader_fieldnames_lower = [fh.lower() for fh in reader.fieldnames] if reader.fieldnames else []
            if not reader_fieldnames_lower or not all(h.lower() in reader_fieldnames_lower for h in expected_headers):
                print(f"    WARNING: CSV headers for {csv_path} might be missing expected columns. Found: {reader.fieldnames}. Attempting to parse.")

            for row in reader:
                full_key = row.get('Key') or row.get('key')
                source_value = row.get('Source') or row.get('source')
                ns_hash_str = row.get('NamespaceHash') or row.get('namespacehash')
                key_hash_str = row.get('KeyHash') or row.get('keyhash')
                source_str_hash_str = row.get('SourceStringHash') or row.get('sourcestringhash')

                if full_key and source_value is not None and \
                   ns_hash_str is not None and key_hash_str is not None and source_str_hash_str is not None:
                    if '/' in full_key:
                        namespace, key_part = full_key.split('/', 1)
                        if not key_part: 
                            key_part = namespace 
                            namespace = ""
                    else:
                        namespace = ""
                        key_part = full_key
                    try:
                        entry_data = {
                            'source': source_value,
                            'ns_hash': int(ns_hash_str),
                            'key_hash': int(key_hash_str),
                            'source_str_hash': int(source_str_hash_str)
                        }
                        data[namespace][key_part] = entry_data
                        contributed_keys += 1
                    except ValueError:
                        print(f"    WARNING: Could not parse hash values as integers for key '{full_key}' in {csv_path}. Row: {row}. Skipping entry.")
            if contributed_keys == 0 and os.path.getsize(csv_path) > 100:
                print(f"    INFO: parse_locres_csv contributed 0 keys from {csv_path}, but file seems to have data. Check CSV format and headers.")
    except Exception as e:
        print(f"    Error parsing LocRes CSV {csv_path}: {e}")
    return data, contributed_keys

def parse_formatstring_txt(file_path, base_formatstring_folder_path):
    data = defaultdict(dict)
    contributed_keys = 0
    try:
        relative_path_to_file = os.path.relpath(file_path, base_formatstring_folder_path)
        path_parts = relative_path_to_file.split(os.sep)
        filename_stem = os.path.splitext(path_parts[-1])[0]
        dir_parts = path_parts[:-1]
        namespace = ""
        key_prefix_parts = []

        if not dir_parts:   
            namespace = filename_stem
        else:   
            namespace = dir_parts[0]
            key_prefix_parts.extend(dir_parts[1:])
            key_prefix_parts.append(filename_stem)
        
        full_key_prefix = "/".join(key_prefix_parts)
        encodings_to_try = ['utf-16', 'utf-8', 'latin-1']
        content_lines = None
        for enc in encodings_to_try:
            try:
                with open(file_path, 'r', encoding=enc) as f:
                    content_lines = f.readlines()
                break
            except (UnicodeDecodeError, UnicodeError):
                continue
        
        if content_lines is None:
            print(f"    ERROR: Could not decode file {file_path} with any attempted encodings. Skipping.")
            return data, 0

        for line_content in content_lines:
            line = line_content.strip()
            if '=' in line:
                file_key, value = line.split('=', 1)
                file_key = file_key.strip().lstrip('\ufeff')
                value = value
                final_key = f"{full_key_prefix}/{file_key}" if full_key_prefix else file_key
                data[namespace][final_key] = value
                contributed_keys += 1
        return data, contributed_keys
    except FileNotFoundError:
        print(f"    ERROR: File not found during parsing: {file_path}")
        return data, 0
    except Exception as e:
        print(f"    Error processing FormatString .txt file {file_path}: {e}")
        return data, 0

def parse_formatstring_json(file_path):
    """
    Parses FormatString JSON files.
    Looks for metadata under both "metaData" (camelCase) and "metadata" (lowercase) keys from the input file.
    Stores it under a consistent "metadata" (lowercase) key internally.
    Expects input format: {"key": {"text": "value", "metaData_or_metadata": {"flags": "...", "note": "..."}}}
    or simpler {"key": "value_string"}
    Returns data as: namespace -> key -> {"text": "...", "metadata": {"flags":"...", "note":"..."}}
    """
    data = defaultdict(lambda: defaultdict(dict)) 
    namespace = os.path.splitext(os.path.basename(file_path))[0] 
    contributed_keys = 0
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = json.load(f)
            if isinstance(content, dict):
                for key, item_data_from_file in content.items():
                    cleaned_key = key.lstrip('\ufeff')
                    
                    text_to_store = None
                    # This will be the structure stored for this item.
                    # Initialize with default metadata.
                    parsed_item_data = {
                        "text": "", 
                        "metadata": {"flags": "", "note": ""}
                    }

                    if isinstance(item_data_from_file, dict) and 'text' in item_data_from_file:
                        text_to_store = item_data_from_file['text']
                        parsed_item_data["text"] = text_to_store
                        
                        # Try to get original metadata, checking both casings
                        original_meta_field = None
                        if 'metaData' in item_data_from_file and isinstance(item_data_from_file['metaData'], dict):
                            original_meta_field = item_data_from_file['metaData']
                        elif 'metadata' in item_data_from_file and isinstance(item_data_from_file['metadata'], dict):
                            original_meta_field = item_data_from_file['metadata']
                        
                        if original_meta_field: # If metadata object was found with either casing
                            parsed_item_data["metadata"]["flags"] = original_meta_field.get("flags", "")
                            parsed_item_data["metadata"]["note"] = original_meta_field.get("note", "")
                        # If original_meta_field is None, parsed_item_data["metadata"] remains default empty
                            
                    elif isinstance(item_data_from_file, str): # Handle simple "key": "value"
                        text_to_store = item_data_from_file
                        parsed_item_data["text"] = text_to_store
                        # metadata remains default empty for this simple case
                    
                    if text_to_store is not None:
                        data[namespace][cleaned_key] = parsed_item_data # Store the full structure
                        contributed_keys += 1
        return data, contributed_keys
    except json.JSONDecodeError as jde:
        print(f"    Error decoding JSON from FormatString file {file_path}: {jde}")
        return data, 0
    except Exception as e:
        print(f"    Error reading FormatString .json file {file_path}: {e}")
        return data, 0

def find_localization_entries(obj, found_data, contributed_keys_ref):
    if isinstance(obj, dict):
        if all(k in obj for k in ["Namespace", "Key", "SourceString"]):
            namespace = obj["Namespace"] if obj["Namespace"] is not None else ""
            key = obj["Key"]
            source_string = obj["SourceString"]
            if key is not None and source_string is not None:
                cleaned_key = key.lstrip('\ufeff')
                if namespace not in found_data:
                    found_data[namespace] = {}
                if cleaned_key not in found_data[namespace]:
                    found_data[namespace][cleaned_key] = source_string
                    contributed_keys_ref[0] += 1 
        for value in obj.values():
            find_localization_entries(value, found_data, contributed_keys_ref)
    elif isinstance(obj, list):
        for item in obj:
            find_localization_entries(item, found_data, contributed_keys_ref)

def process_ui_asset_json_file(file_path):
    data = defaultdict(dict)    
    contributed_keys_ref = [0]    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = json.load(f)
            find_localization_entries(content, data, contributed_keys_ref)    
        return data, contributed_keys_ref[0]
    except json.JSONDecodeError as jde:
        print(f"    Error decoding JSON from UI Asset file {file_path}: {jde}")
        return data, 0
    except Exception as e:
        print(f"    Error processing UI Asset JSON file {file_path}: {e}")
        return data, 0

def process_locres_folder(folder_path_for_lang, temp_csv_dir):
    combined_data = defaultdict(lambda: defaultdict(dict))
    total_contributed_keys = 0
    locres_file = os.path.join(folder_path_for_lang, "Game.locres")
    if os.path.exists(locres_file):
        print(f"  Processing .locres: {locres_file}")
        sanitized_folder_name = "".join(c if c.isalnum() else "_" for c in os.path.basename(os.path.normpath(folder_path_for_lang)))
        sanitized_locres_name = "".join(c if c.isalnum() else "_" for c in os.path.basename(locres_file))
        temp_csv_name = f"{sanitized_folder_name}_{sanitized_locres_name}.csv"
        output_csv_path = os.path.join(temp_csv_dir, temp_csv_name)
        if run_unreal_locres(locres_file, output_csv_path):
            parsed_data, contributed = parse_locres_csv(output_csv_path)
            if parsed_data:
                total_contributed_keys += contributed
                for ns, items in parsed_data.items():
                    if ns not in combined_data: combined_data[ns] = {}
                    combined_data[ns].update(items)
        else:
            print(f"  Failed to convert .locres to CSV for {locres_file}")
    else:
        print(f"  INFO: Locres file not found at {locres_file}.")
    return combined_data, total_contributed_keys

def process_generic_folder(folder_path, file_extension, parser_function, pass_base_folder_to_parser=False):
    combined_data = defaultdict(dict)
    total_contributed_keys = 0
    if not os.path.isdir(folder_path):
        print(f"  WARNING: Folder not found: {folder_path}")
        return combined_data, total_contributed_keys
    print(f"  Processing folder: {folder_path} for *.{file_extension}")
    for root, _, files in os.walk(folder_path):
        for filename in files:
            if filename.lower().endswith(file_extension):
                file_path = os.path.join(root, filename)
                if pass_base_folder_to_parser:
                    parsed_data, contributed = parser_function(file_path, folder_path)    
                else:
                    parsed_data, contributed = parser_function(file_path)
                if parsed_data:
                    total_contributed_keys += contributed
                    for ns, items in parsed_data.items():
                        if ns not in combined_data: combined_data[ns] = {}
                        combined_data[ns].update(items)
    return combined_data, total_contributed_keys

# --- Main Script ---
def main():
    if not cityhash:
        print("CRITICAL ERROR: cityhash library is not installed or failed to import.")
        print("Please install it: pip install cityhash")
        return

    statistics = defaultdict(lambda: defaultdict(int)) 
    temp_dir_obj = tempfile.TemporaryDirectory()
    temp_csv_dir_path = temp_dir_obj.name
    print(f"Using temporary directory for CSVs: {temp_csv_dir_path}")

    all_extracted_data_by_source = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(dict))))

    for version_name in GAME_VERSIONS_ORDER:
        print(f"\n--- Gathering data for game version: {version_name} ---")
        for source_type_name, config in SOURCE_CONFIG.items():
            print(f"  Processing source type: {source_type_name}")
            handler_func_name = config["handler"]
            base_folder_for_pak_group = os.path.join(EXTRACTED_DATA_DIR, version_name, config["pak_group"])
            
            data = {}
            count = 0

            if handler_func_name == "process_locres_folder":
                # LocRes is directly in pak_group folder, not a subfolder named "Localization" etc.
                data, count = process_locres_folder(base_folder_for_pak_group, temp_csv_dir_path)
            elif handler_func_name == "parse_formatstring_txt":
                actual_folder = os.path.join(base_folder_for_pak_group, config.get("subfolder", ""))
                data, count = process_generic_folder(actual_folder, ".txt", parse_formatstring_txt, pass_base_folder_to_parser=True)
            elif handler_func_name == "parse_formatstring_json":
                actual_folder = os.path.join(base_folder_for_pak_group, config.get("subfolder", ""))
                data, count = process_generic_folder(actual_folder, ".json", parse_formatstring_json)
            elif handler_func_name == "process_ui_assets_folder":
                actual_folder = os.path.join(base_folder_for_pak_group, config.get("subfolder", ""))
                data, count = process_generic_folder(actual_folder, ".uasset.json", process_ui_asset_json_file)
            
            all_extracted_data_by_source[version_name][source_type_name] = data
            print(f"    {source_type_name} for {version_name} initially found {count} entries.")


    # --- Create Final Unified JSON (for translation input) ---
    final_unified_json = defaultdict(dict)
    sorted_source_types_for_json_merge = sorted(SOURCE_CONFIG.keys(), key=lambda k: SOURCE_CONFIG[k]["priority"])

    for version_name in GAME_VERSIONS_ORDER: 
        print(f"\n--- Merging data for FINAL UNIFIED JSON from game version: {version_name} ---")
        for source_type in sorted_source_types_for_json_merge: 
            print(f"  Merging source for JSON: {source_type} (Priority: {SOURCE_CONFIG[source_type]['priority']})")
            data_for_source = all_extracted_data_by_source[version_name][source_type]
            
            for namespace, kv_pairs in data_for_source.items():
                if namespace not in final_unified_json:
                    final_unified_json[namespace] = {}
                
                for key, value_or_entry_data in kv_pairs.items():
                    actual_value = value_or_entry_data
                    if source_type == "LocRes": 
                        actual_value = value_or_entry_data.get('source', '')
                    elif source_type == "FormatString_Json": # MODIFIED
                        actual_value = value_or_entry_data.get('text', '') # Get the text part
                    # For FormatString_Txt and UI_Assets, value_or_entry_data is already the string value

                    final_unified_json[namespace][key] = actual_value

    # Calculate statistics
    final_stat_provider = {} 
    for version_name_iter in GAME_VERSIONS_ORDER:
        for source_type_iter in sorted_source_types_for_json_merge:
            data_for_source = all_extracted_data_by_source[version_name_iter][source_type_iter]
            for namespace_iter, kv_pairs_iter in data_for_source.items():
                for key_iter in kv_pairs_iter.keys():
                    final_stat_provider[(namespace_iter, key_iter)] = (version_name_iter, source_type_iter)
    for (ns_stat, k_stat), (v_name_stat, s_type_stat) in final_stat_provider.items():
        if ns_stat in final_unified_json and k_stat in final_unified_json[ns_stat]:
             statistics[v_name_stat][s_type_stat] += 1

    try:
        with open(FINAL_JSON_OUTPUT_PATH, 'w', encoding='utf-8') as outfile:
            json.dump(final_unified_json, outfile, indent=4, ensure_ascii=False)
        print(f"\nUnified JSON data successfully written to: {FINAL_JSON_OUTPUT_PATH}")
    except Exception as e:
        print(f"Error writing final JSON output: {e}")

    # --- Determine and store all original source types for each unified key ---
    print(f"\n--- Determining all original source types for each unified key (sources from latest version only) ---")
    # unified_key_source_origins will store: namespace -> key -> list_of_sources
    # where list_of_sources are from the LATEST game version that contained that key.
    unified_key_source_origins = defaultdict(lambda: defaultdict(list)) 

    # To keep track of the version index that provided the sources for a key
    # (ns, key) -> version_idx (index in GAME_VERSIONS_ORDER)
    key_version_tracker = {} 

    for version_idx, version_name in enumerate(GAME_VERSIONS_ORDER): # e.g., 0: ZXSJ_Speed, 1: zxsjgt
        print(f"  Processing sources from version: {version_name} (Index: {version_idx})")
        for source_type_name, config_details in SOURCE_CONFIG.items():
            data_for_this_source_and_version = all_extracted_data_by_source[version_name].get(source_type_name, {})
            for namespace, kv_pairs in data_for_this_source_and_version.items():
                for key, data_item_from_source in kv_pairs.items(): 
                    current_key_tuple = (namespace, key)

                    # Check if this version should be the new authority for this key's sources
                    if current_key_tuple not in key_version_tracker or \
                    version_idx > key_version_tracker[current_key_tuple]:
                        # This version is newer or it's the first time seeing this key.
                        # Clear any sources from previous, older versions for this specific key.
                        unified_key_source_origins[namespace][key] = [] 
                        key_version_tracker[current_key_tuple] = version_idx
                    
                    # Only add sources if the current version_idx matches the tracked provider version_idx for this key
                    # This ensures we only add sources from the "winning" version for this key.
                    if key_version_tracker[current_key_tuple] == version_idx:
                        source_to_add = None
                        if source_type_name == "FormatString_Json":
                            # data_item_from_source is expected to be {"text": ..., "metadata": ...}
                            # as prepared by the updated parse_formatstring_json
                            metadata_obj = data_item_from_source.get('metadata', {"flags": "", "note": ""})
                            source_to_add = {"type": "FormatString_Json", "metadata": metadata_obj}
                        else:
                            source_to_add = source_type_name # e.g., "LocRes", "FormatString_Txt", "UI_Assets"
                        # Add to the list for this ns/key, ensuring uniqueness for this version's contribution
                        current_sources_list_for_key = unified_key_source_origins[namespace][key]
                        is_present = False
                        if isinstance(source_to_add, dict):
                            for existing_item in current_sources_list_for_key:
                                if isinstance(existing_item, dict) and \
                                existing_item.get("type") == source_to_add.get("type") and \
                                existing_item.get("metadata") == source_to_add.get("metadata"): # Basic dict equality for metadata
                                    is_present = True
                                    break
                        elif source_to_add in current_sources_list_for_key:
                            is_present = True
                        
                        if not is_present:
                            current_sources_list_for_key.append(source_to_add)

    total_unique_ns_keys_with_sources = sum(len(keys_map) for keys_map in unified_key_source_origins.values())
    print(f"  Processed source origins for {total_unique_ns_keys_with_sources} unique (namespace, key) pairs, reflecting sources from the latest relevant version.")

    # Convert to final structure for JSON serialization (sorting for consistency)
    serializable_source_origins = defaultdict(dict)
    sorted_namespaces_for_origins = sorted(unified_key_source_origins.keys()) 
    for ns in sorted_namespaces_for_origins:
        keys_data_map = unified_key_source_origins[ns]
        serializable_source_origins[ns] = {} 
        sorted_keys_for_origins = sorted(keys_data_map.keys()) 
        for k in sorted_keys_for_origins:
            list_of_sources = keys_data_map[k]
            # Attempt to sort the list of sources if possible (e.g., if all strings)
            # For mixed lists (strings and dicts), direct sorting will fail.
            # The order within this small list is not hyper-critical for functionality.
            try:
                if all(isinstance(item, str) for item in list_of_sources):
                    serializable_source_origins[ns][k] = sorted(list_of_sources)
                else: # Mixed list, or list with dicts, keep current order
                    serializable_source_origins[ns][k] = list_of_sources
            except TypeError: 
                serializable_source_origins[ns][k] = list_of_sources # Fallback

    try:
        with open(KEY_SOURCE_ORIGINS_JSON_PATH, 'w', encoding='utf-8') as outfile:
            json.dump(serializable_source_origins, outfile, indent=4, ensure_ascii=False)
        print(f"Key source origins (latest version priority) successfully written to: {KEY_SOURCE_ORIGINS_JSON_PATH}")
    except Exception as e:
        print(f"Error writing key source origins JSON: {e}")

    # The rest of the main() function (creating FINAL_JSON_OUTPUT_PATH, 
    # UNIFIED_LOCRES_HASH_CSV_PATH, stats) remains the same as in your provided script.


    # --- Create the new Unified LocRes CSV with Hashes ---
    print(f"\n--- Preparing Unified LocRes CSV with Hashes ---")
    unified_locres_for_csv = {} 
    ui_added_to_csv_count = 0

    locres_source_key = "LocRes"
    for version_name in GAME_VERSIONS_ORDER: 
        print(f"  Processing LocRes data for CSV from game version: {version_name}")
        version_locres_data = all_extracted_data_by_source[version_name].get(locres_source_key, {})
        for namespace, keys_data in version_locres_data.items():
            for key_part, entry_data in keys_data.items(): 
                unified_locres_for_csv[(namespace, key_part)] = entry_data

    ui_source_key = "UI_Assets"
    print(f"\n  Attempting to augment CSV with {ui_source_key} data...")
    for version_name in GAME_VERSIONS_ORDER:
        print(f"  Processing {ui_source_key} from game version: {version_name} for CSV augmentation")
        version_ui_data = all_extracted_data_by_source[version_name].get(ui_source_key, {})
        for namespace, keys_data in version_ui_data.items():
            for key, source_value_ui in keys_data.items(): # source_value_ui is the string from UI asset
                if (namespace, key) not in unified_locres_for_csv: 
                    try:
                        source_str_hash = calculate_source_string_hash(source_value_ui)
                        key_hash = calculate_optimized_cityhash64_utf16_key_hash(key)
                        ns_hash = 0 if namespace == "" else calculate_optimized_cityhash64_utf16_key_hash(namespace)
                        entry_data_for_ui = {
                            'source': source_value_ui,
                            'ns_hash': ns_hash,
                            'key_hash': key_hash,
                            'source_str_hash': source_str_hash 
                        }
                        unified_locres_for_csv[(namespace, key)] = entry_data_for_ui
                        ui_added_to_csv_count +=1
                    except Exception as e:
                        print(f"    ERROR: Could not calculate hashes for UI asset key '{namespace}/{key}': {e}")
    
    print(f"  Added {ui_added_to_csv_count} new entries from UI_Assets to the LocRes CSV data.")

    try:
        with open(UNIFIED_LOCRES_HASH_CSV_PATH, 'w', encoding='utf-8-sig', newline='') as csvfile:
            fieldnames = ['Namespace', 'Key', 'SourceValue', 'NamespaceHash', 'KeyHash_of_KeyString', 'SourceStringHash_of_SourceText']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            sorted_locres_csv_keys = sorted(unified_locres_for_csv.keys()) 

            for (namespace, key_part) in sorted_locres_csv_keys:
                entry = unified_locres_for_csv[(namespace, key_part)]
                writer.writerow({
                    'Namespace': namespace,
                    'Key': key_part,
                    'SourceValue': entry.get('source'), 
                    'NamespaceHash': entry.get('ns_hash'),
                    'KeyHash_of_KeyString': entry.get('key_hash'),
                    'SourceStringHash_of_SourceText': entry.get('source_str_hash')
                })
        print(f"Unified LocRes CSV with hashes successfully written to: {UNIFIED_LOCRES_HASH_CSV_PATH}")
        print(f"  Total unique LocRes-capable entries in this CSV: {len(unified_locres_for_csv)}")

    except Exception as e:
        print(f"Error writing unified LocRes CSV with hashes: {e}")
    finally:
        try:
            print(f"Cleaning up temporary directory: {temp_csv_dir_path}")
            temp_dir_obj.cleanup()
        except Exception as e:
            print(f"Error cleaning up temporary directory {temp_csv_dir_path}: {e}")

    print("\n--- Contribution Statistics (based on final_stat_provider) ---")
    for version_name_stat in GAME_VERSIONS_ORDER:    
        print(f"  Game Version: {version_name_stat}")
        total_for_version = 0
        for source_type_stat in sorted_source_types_for_json_merge: 
            count = statistics[version_name_stat].get(source_type_stat, 0)
            print(f"    {source_type_stat}: {count} keys")
            total_for_version += count
        print(f"    --------------------")
        print(f"    Total unique keys for {version_name_stat} (whose value was provided by this version): {total_for_version} keys")

    grand_total_unique_keys_in_json = 0
    for namespace_content in final_unified_json.values():
        grand_total_unique_keys_in_json += len(namespace_content)
    print(f"\nGrand total unique keys in '{FINAL_JSON_OUTPUT_PATH}': {grand_total_unique_keys_in_json}")


if __name__ == "__main__":
    if not os.path.isdir(EXTRACTED_DATA_DIR):
        print(f"ERROR: Base extracted data directory not found: {EXTRACTED_DATA_DIR}")
        print("Please run the get_files.py script first to generate the input for this script.")
    elif not os.path.exists(UNREAL_LOCRES_EXE_PATH) or not os.path.isfile(UNREAL_LOCRES_EXE_PATH):
        print(f"ERROR: UnrealLocres.exe not found at '{UNREAL_LOCRES_EXE_PATH}'. Please place it in the same directory as this script.")
    else:
        main()