import json
import os
import math
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import re
import threading
from collections import defaultdict, Counter
import time
import sys

# --- Configuration & Constants ---
CHINESE_CHAR_REGEX = re.compile(r'[\u4e00-\u9fff]')
TAG_REGEX = re.compile(r'(<[^>]+>|\(##Color:[^)]+\)|\(@@[^)]+\)|\(\s*/\s*[^)]+\))')
CONDITIONAL_BLOCK_REGEX = re.compile(r'\(\s*@@([^)]+)\).*?\(\s*/\1\s*\)', re.DOTALL)
OPENUI_REGEX = re.compile(r'\(\s*@@OpenUI\s*\)(.*?)\|(.*?)\(\s*/OpenUI\s*\)', re.DOTALL)

ISSUE_CHINESE_IN_VALUE = "Chinese Chars in Value"
ISSUE_TAG_MISMATCH = "Tag Mismatch (Source Key vs. Value)"
ISSUE_CONDITIONAL_MISMATCH = "Conditional Content Mismatch (Source Key vs. Value)"

# --- Core Helper Functions (Non-GUI) ---

def estimate_json_size(data_dict):
    try:
        return len(json.dumps(data_dict, ensure_ascii=False, indent=4).encode('utf-8'))
    except TypeError: return float('inf')

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', s)]

def extract_tags(text):
    if not isinstance(text, str): return []
    try: return TAG_REGEX.findall(text)
    except Exception: return []

def normalize_tag_structure(tags_list):
    normalized = []
    for tag in tags_list:
        if not isinstance(tag, str): normalized.append('<??>'); continue
        if tag.startswith('</') and tag.endswith('>'): normalized.append('</>')
        elif tag.startswith('( /') and tag.endswith(')'): normalized.append('( / )')
        elif tag.startswith('<') and tag.endswith('>'): normalized.append('<>')
        elif tag.startswith('(##Color:'): normalized.append('(##Color)')
        elif tag.startswith('(@@'): normalized.append('(@@)')
        else: normalized.append('<??>')
    return normalized

def normalize_conditional_block(block_string):
    match = OPENUI_REGEX.match(block_string)
    if match: return match.group(2).strip()
    return block_string

class SearchRule:
    def __init__(self, raw_input, rule_type, key_part, value_exclusion_part=None):
        self.raw_input, self.rule_type, self.key_part, self.value_exclusion_part = \
            raw_input, rule_type, key_part, value_exclusion_part
    def __str__(self): # For debugging
        return f"SearchRule(raw='{self.raw_input}', type='{self.rule_type}', key_part='{self.key_part}', value_excl='{self.value_exclusion_part}')"

# --- Core Logic Functions ---

def do_split_json(input_filepath, output_dir, max_size_kb, status_callback):
    status_callback(f"Splitting {input_filepath} into parts <= {max_size_kb}KB in {output_dir}")
    try:
        with open(input_filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, dict):
            status_callback("Error: Input JSON for splitting must be an object (dictionary).")
            messagebox.showerror("Split Error", "Input JSON for splitting must be an object (dictionary).")
            return False

        max_size_bytes = max_size_kb * 1024
        if max_size_bytes <= 50:
            status_callback("Warning: Max size is very small. Ensure it's large enough for basic JSON structure.")
            if max_size_bytes <= 0:
                status_callback("Error: Max size must be positive.")
                messagebox.showerror("Split Error", "Max size must be a positive number.")
                return False

        os.makedirs(output_dir, exist_ok=True)
        output_filename_base = os.path.splitext(os.path.basename(input_filepath))[0]
        if output_filename_base.endswith('_cleared'):
            output_filename_base = output_filename_base[:-len('_cleared')]

        file_index = 1
        current_file_dict = {}
        original_keys = list(data.keys())
        status_callback(f"Processing {len(original_keys)} top-level keys...")
        chunking_iterators = {}
        key_idx = 0
        written_files_count = 0

        while key_idx < len(original_keys) or chunking_iterators:
            processed_chunk_this_iteration = False
            keys_being_chunked = list(chunking_iterators.keys())
            for key_being_chunked in keys_being_chunked:
                iterator = chunking_iterators[key_being_chunked]
                current_key_data_in_file = current_file_dict.get(key_being_chunked)
                try:
                    next_item_from_iterator = next(iterator)
                    item_to_add_to_key = None
                    new_data_for_key_in_file = None

                    if isinstance(current_key_data_in_file, list):
                        item_to_add_to_key = next_item_from_iterator
                        new_data_for_key_in_file = current_key_data_in_file + [item_to_add_to_key]
                    elif isinstance(current_key_data_in_file, dict):
                        item_to_add_to_key = {next_item_from_iterator[0]: next_item_from_iterator[1]}
                        new_data_for_key_in_file = current_key_data_in_file.copy()
                        new_data_for_key_in_file.update(item_to_add_to_key)
                    elif current_key_data_in_file is None:
                        if isinstance(data[key_being_chunked], list):
                            item_to_add_to_key = next_item_from_iterator
                            new_data_for_key_in_file = [item_to_add_to_key]
                        elif isinstance(data[key_being_chunked], dict):
                            item_to_add_to_key = {next_item_from_iterator[0]: next_item_from_iterator[1]}
                            new_data_for_key_in_file = item_to_add_to_key

                    if new_data_for_key_in_file is not None:
                        temp_file_dict_check = current_file_dict.copy()
                        temp_file_dict_check[key_being_chunked] = new_data_for_key_in_file
                        estimated_size = estimate_json_size(temp_file_dict_check)

                        if estimated_size <= max_size_bytes:
                            current_file_dict[key_being_chunked] = new_data_for_key_in_file
                            processed_chunk_this_iteration = True
                        else:
                            if current_file_dict:
                                output_filepath_part = os.path.join(output_dir, f"{output_filename_base}_part_{file_index}.json")
                                with open(output_filepath_part, 'w', encoding='utf-8') as f_out:
                                    json.dump(current_file_dict, f_out, ensure_ascii=False, indent=4)
                                status_callback(f" > Wrote {os.path.basename(output_filepath_part)} ({estimate_json_size(current_file_dict)/1024:.2f} KB)")
                                written_files_count+=1
                                file_index += 1
                            current_file_dict = {}
                            if isinstance(data[key_being_chunked], list):
                                current_file_dict[key_being_chunked] = [item_to_add_to_key]
                            elif isinstance(data[key_being_chunked], dict):
                                current_file_dict[key_being_chunked] = item_to_add_to_key if isinstance(item_to_add_to_key, dict) else {next_item_from_iterator[0]: next_item_from_iterator[1]}
                            processed_chunk_this_iteration = True
                    else:
                        status_callback(f"Error: Type mismatch or unhandled case during chunking for key '{key_being_chunked}'.")
                        del chunking_iterators[key_being_chunked]
                except StopIteration:
                    status_callback(f"   * Finished chunking for key '{key_being_chunked}'")
                    del chunking_iterators[key_being_chunked]
                    processed_chunk_this_iteration = True
                if processed_chunk_this_iteration: break
            if processed_chunk_this_iteration: continue

            if key_idx < len(original_keys):
                original_key_to_add = original_keys[key_idx]
                original_value_to_add = data[original_key_to_add]
                temp_file_dict_check = current_file_dict.copy()
                temp_file_dict_check[original_key_to_add] = original_value_to_add
                estimated_size = estimate_json_size(temp_file_dict_check)

                if estimated_size <= max_size_bytes:
                    current_file_dict[original_key_to_add] = original_value_to_add
                    status_callback(f"   + Added key '{original_key_to_add}' (whole)")
                    key_idx += 1
                else:
                    if current_file_dict:
                        output_filepath_part = os.path.join(output_dir, f"{output_filename_base}_part_{file_index}.json")
                        with open(output_filepath_part, 'w', encoding='utf-8') as f_out:
                            json.dump(current_file_dict, f_out, ensure_ascii=False, indent=4)
                        status_callback(f" > Wrote {os.path.basename(output_filepath_part)} ({estimate_json_size(current_file_dict)/1024:.2f} KB)")
                        written_files_count+=1
                        file_index += 1
                        current_file_dict = {}
                    if isinstance(original_value_to_add, (list, dict)) and original_value_to_add:
                        first_element_data_for_check = {}
                        can_start_chunking = False
                        if isinstance(original_value_to_add, list):
                            if original_value_to_add:
                                first_element_data_for_check = {original_key_to_add: [original_value_to_add[0]]}
                                can_start_chunking = True
                        elif isinstance(original_value_to_add, dict):
                            if original_value_to_add:
                                first_sub_key = next(iter(original_value_to_add))
                                first_element_data_for_check = {original_key_to_add: {first_sub_key: original_value_to_add[first_sub_key]}}
                                can_start_chunking = True

                        if can_start_chunking and estimate_json_size(first_element_data_for_check) <= max_size_bytes:
                            status_callback(f"   ! Key '{original_key_to_add}' value is large, starting chunking...")
                            if isinstance(original_value_to_add, list):
                                chunking_iterators[original_key_to_add] = iter(original_value_to_add)
                            else:
                                chunking_iterators[original_key_to_add] = iter(original_value_to_add.items())
                            key_idx += 1
                        else:
                            size_alone_kb = estimate_json_size({original_key_to_add: original_value_to_add})/1024
                            status_callback(f"Warning: Key '{original_key_to_add}' value (or its first element {size_alone_kb:.2f} KB) is too large "
                                            "and cannot be chunked. Writing it alone.")
                            output_filepath_part = os.path.join(output_dir, f"{output_filename_base}_part_{file_index}.json")
                            with open(output_filepath_part, 'w', encoding='utf-8') as f_out:
                                json.dump({original_key_to_add: original_value_to_add}, f_out, ensure_ascii=False, indent=4)
                            status_callback(f" > Wrote oversized key {os.path.basename(output_filepath_part)}")
                            written_files_count+=1
                            file_index += 1
                            key_idx += 1
                            current_file_dict = {}
                    else:
                        status_callback(f"Warning: Key '{original_key_to_add}' value is not chunkable or too large. Writing alone.")
                        output_filepath_part = os.path.join(output_dir, f"{output_filename_base}_part_{file_index}.json")
                        with open(output_filepath_part, 'w', encoding='utf-8') as f_out:
                            json.dump({original_key_to_add: original_value_to_add}, f_out, ensure_ascii=False, indent=4)
                        status_callback(f" > Wrote oversized unchunkable key {os.path.basename(output_filepath_part)}")
                        written_files_count+=1
                        file_index += 1
                        key_idx += 1
                        current_file_dict = {}
        if current_file_dict:
            output_filepath_part = os.path.join(output_dir, f"{output_filename_base}_part_{file_index}.json")
            with open(output_filepath_part, 'w', encoding='utf-8') as f_out:
                json.dump(current_file_dict, f_out, ensure_ascii=False, indent=4)
            status_callback(f" > Wrote final file {os.path.basename(output_filepath_part)} ({estimate_json_size(current_file_dict)/1024:.2f} KB)")
            written_files_count+=1
        if not written_files_count and (key_idx == len(original_keys) and not chunking_iterators):
            status_callback("\nWarning: Input JSON was empty or resulted in no output files.")
        elif written_files_count:
            status_callback(f"\nSplitting complete. {written_files_count} files written to {output_dir}")
        return True
    except Exception as e:
        status_callback(f"Error during split: {e}")
        import traceback; status_callback(traceback.format_exc())
        messagebox.showerror("Split Error", str(e))
        return False

def do_merge_json_parts(input_dir, output_filepath, status_callback):
    status_callback(f"Merging parts from {input_dir} into {output_filepath}")
    try:
        merged_data = {}
        all_files_in_dir = os.listdir(input_dir)
        part_files = [f for f in all_files_in_dir if f.lower().endswith('.json') and "_part_" in f.lower()]
        if not part_files:
            status_callback("No files matching '_part_X.json' pattern found. Trying all .json files.")
            part_files = [f for f in all_files_in_dir if f.lower().endswith('.json')]
        part_files = sorted(part_files, key=natural_sort_key)
        if not part_files:
            status_callback("No JSON files found to merge in the specified directory.")
            messagebox.showinfo("Merge Info", "No JSON files found in the input directory.")
            return False
        status_callback(f"Found {len(part_files)} files to merge: {', '.join(part_files[:5])}{'...' if len(part_files) > 5 else ''}")
        for filename in part_files:
            filepath = os.path.join(input_dir, filename)
            status_callback(f" > Processing: {filename}...")
            try:
                with open(filepath, 'r', encoding='utf-8') as f_part:
                    part_data = json.load(f_part)
                if not isinstance(part_data, dict):
                    status_callback(f"Warning: Skipping non-dictionary file: {filename}")
                    continue
                for key, value in part_data.items():
                    if key in merged_data:
                        existing_value = merged_data[key]
                        if isinstance(existing_value, list) and isinstance(value, list):
                            existing_value.extend(value)
                        elif isinstance(existing_value, dict) and isinstance(value, dict):
                            existing_value.update(value)
                        else:
                            status_callback(f"Warning: Overwriting key '{key}' from {filename} (type mismatch or simple value).")
                            merged_data[key] = value
                    else:
                        merged_data[key] = value
            except json.JSONDecodeError as e: status_callback(f"Error decoding JSON from {filename}: {e}. Skipping file.")
            except Exception as e: status_callback(f"Error processing file {filename}: {e}. Skipping file.")
        if not merged_data and part_files:
            status_callback("Warning: Merged data is empty after processing files. Output file will be empty or not created if error.")

        os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
        with open(output_filepath, 'w', encoding='utf-8') as f_out:
            json.dump(merged_data, f_out, ensure_ascii=False, indent=4)
        merged_size_kb = estimate_json_size(merged_data) / 1024
        status_callback(f"Merging complete. Merged file size: {merged_size_kb:.2f} KB. Saved to {output_filepath}")
        return True
    except Exception as e:
        status_callback(f"Error during merge: {e}")
        import traceback; status_callback(traceback.format_exc())
        messagebox.showerror("Merge Error", str(e))
        return False

def _recursive_quality_check(current_data, current_path, problems_list, checks_config):
    if isinstance(current_data, dict):
        for key, value in current_data.items():
            item_path = f"{current_path}.{key}" if current_path else key
            if isinstance(key, str) and isinstance(value, str):
                source_text = key; target_text = value
                if checks_config.get("chinese") and CHINESE_CHAR_REGEX.search(target_text):
                    problems_list.append({"path": item_path, "key": source_text, "value": target_text, "issue": ISSUE_CHINESE_IN_VALUE, "details": "Value contains Chinese chars."})
                if checks_config.get("tags"):
                    s_tags, t_tags = extract_tags(source_text), extract_tags(target_text)
                    norm_s, norm_t = normalize_tag_structure(s_tags), normalize_tag_structure(t_tags)
                    s_counts, t_counts = Counter(norm_s), Counter(norm_t)
                    mismatches = [f"Missing {count - t_counts[tag_type]} of '{tag_type}' tags" for tag_type, count in s_counts.items() if t_counts[tag_type] < count]
                    if mismatches: problems_list.append({"path": item_path, "key": source_text, "value": target_text, "issue": ISSUE_TAG_MISMATCH, "details": "; ".join(mismatches)})
                if checks_config.get("conditional"):
                    s_blocks = [normalize_conditional_block(m.group(0)) for m in CONDITIONAL_BLOCK_REGEX.finditer(source_text)]
                    t_blocks = [normalize_conditional_block(m.group(0)) for m in CONDITIONAL_BLOCK_REGEX.finditer(target_text)]
                    if Counter(s_blocks) != Counter(t_blocks):
                        problems_list.append({"path": item_path, "key": source_text, "value": target_text, "issue": ISSUE_CONDITIONAL_MISMATCH, "details": f"Src blocks: {Counter(s_blocks)}, Val blocks: {Counter(t_blocks)}"})
            if isinstance(value, (dict, list)): _recursive_quality_check(value, item_path, problems_list, checks_config)
    elif isinstance(current_data, list):
        for i, item in enumerate(current_data):
            item_path = f"{current_path}[{i}]"
            if isinstance(item, (dict, list)): _recursive_quality_check(item, item_path, problems_list, checks_config)
            elif checks_config.get("chinese") and isinstance(item, str) and CHINESE_CHAR_REGEX.search(item):
                problems_list.append({"path": item_path, "key": "(List Item)", "value": item, "issue": ISSUE_CHINESE_IN_VALUE, "details": "List item string contains Chinese chars."})

def do_quality_checks(json_data, checks_config, status_callback):
    status_callback("Starting quality checks...")
    problems_found = []
    if not json_data: status_callback("No JSON data loaded for quality checks."); return problems_found
    _recursive_quality_check(json_data, "", problems_found, checks_config)
    status_callback(f"Quality checks core logic complete. Found {len(problems_found)} potential issues.")
    return problems_found

def deep_merge_dicts(main_dict, external_dict):
    for key, ext_val in external_dict.items():
        main_val = main_dict.get(key)
        if isinstance(main_val, dict) and isinstance(ext_val, dict): deep_merge_dicts(main_val, ext_val)
        else: main_dict[key] = ext_val
    return main_dict

def do_merge_external_json(main_filepath, external_filepath, status_callback):
    status_callback(f"Loading main file: {main_filepath}")
    try:
        with open(main_filepath, 'r', encoding='utf-8') as f: main_data = json.load(f)
        if not isinstance(main_data, dict): status_callback("Error: Main JSON file's root is not a dictionary."); messagebox.showerror("Merge Error", "Main JSON file's root must be a dictionary."); return False
        status_callback(f"Loading external file: {external_filepath}")
        with open(external_filepath, 'r', encoding='utf-8') as f: external_data = json.load(f)
        if not isinstance(external_data, dict): status_callback("Error: External JSON file's root is not a dictionary."); messagebox.showerror("Merge Error", "External JSON file's root must be a dictionary."); return False
        status_callback("Performing merge...")
        merged_data = deep_merge_dicts(main_data, external_data)
        save_path = filedialog.asksaveasfilename(title="Save Merged File As...", initialfile=os.path.basename(main_filepath), initialdir=os.path.dirname(main_filepath), defaultextension=".json", filetypes=[("JSON files", "*.json"), ("All files", "*.*")])
        if not save_path: status_callback("Merge complete, but save cancelled by user."); return True
        with open(save_path, 'w', encoding='utf-8') as f_out: json.dump(merged_data, f_out, ensure_ascii=False, indent=4)
        status_callback(f"Merge complete. Saved to {save_path}"); return True
    except FileNotFoundError as e: status_callback(f"Error: File not found - {e.filename}"); messagebox.showerror("File Error", f"File not found:\n{e.filename}")
    except json.JSONDecodeError as e: status_callback(f"Error decoding JSON: {e}"); messagebox.showerror("JSON Error", f"Invalid JSON format encountered:\n{e}")
    except Exception as e: status_callback(f"An unexpected error occurred during merge: {e}"); import traceback; status_callback(traceback.format_exc()); messagebox.showerror("Merge Error", f"An unexpected error occurred:\n{e}")
    return False

def _recursive_extraction(current_data, current_path, rules, extracted_entries_list, rule_match_counts_dict):
    if isinstance(current_data, dict):
        for key, value in current_data.items():
            entry_path = f"{current_path}.{key}" if current_path else key
            # Check if this key-value pair itself matches rules
            if isinstance(key, str): # Ensure key is string for matching
                value_for_check = str(value) if isinstance(value, (str, int, float, bool)) else "" # Make value string for checking if it's simple type
                for rule in rules:
                    entry_matches_this_rule = False
                    key_contains_key_part = rule.key_part in key
                    if rule.rule_type == 'key_only':
                        if key_contains_key_part:
                            entry_matches_this_rule = True
                    elif rule.rule_type == 'key_value_negative':
                        # Value exclusion part should only be checked if value is a string
                        # If value is dict/list, it won't match a simple string exclusion directly
                        value_is_simple_str_for_check = isinstance(value, str)
                        if key_contains_key_part:
                            if value_is_simple_str_for_check:
                                if not (rule.value_exclusion_part and rule.value_exclusion_part.lower() in value.lower()):
                                    entry_matches_this_rule = True
                            else: # Key matches, value is not a string (e.g. dict/list), so exclusion doesn't apply in the simple sense
                                entry_matches_this_rule = True


                    if entry_matches_this_rule:
                        extracted_entries_list.append({"path": entry_path, "key": key, "value": value})
                        rule_match_counts_dict[rule.raw_input] = rule_match_counts_dict.get(rule.raw_input, 0) + 1
                        break # Stop checking other rules for this item if one matches
            
            # Recurse if value is a collection
            if isinstance(value, (dict, list)):
                _recursive_extraction(value, entry_path, rules, extracted_entries_list, rule_match_counts_dict)
    elif isinstance(current_data, list):
        for i, item in enumerate(current_data):
            entry_path = f"{current_path}[{i}]"
            # Lists themselves don't have "keys" in the JSON sense for rule matching directly here.
            # Rules typically apply to key-value pairs within dicts.
            # However, we must recurse into complex items within lists.
            if isinstance(item, (dict, list)):
                _recursive_extraction(item, entry_path, rules, extracted_entries_list, rule_match_counts_dict)
            # If item is a simple string in a list and a rule could potentially match it (e.g. if rules were extended for list items):
            # For now, the provided rules are key-based, so direct string matching in lists isn't covered by SearchRule types.

def do_substring_extraction(json_data, search_rules_text, status_callback):
    status_callback("Parsing search rules...")
    raw_lines = [line.strip() for line in search_rules_text.splitlines() if line.strip() and not line.strip().startswith('#')]
    if not raw_lines: status_callback("No search rules provided."); messagebox.showinfo("Extraction", "No search rules provided."); return [], {}
    parsed_rules = []
    for line in raw_lines:
        parts = [p.strip() for p in line.split(',', 1)]
        if len(parts) == 2 and parts[0] and parts[1]: parsed_rules.append(SearchRule(line, 'key_value_negative', parts[0], parts[1]))
        elif len(parts) == 1 and parts[0]: parsed_rules.append(SearchRule(line, 'key_only', parts[0]))
        else: status_callback(f"Warning: Skipping invalid rule format: '{line}'")
    if not parsed_rules: status_callback("No valid search rules parsed."); messagebox.showinfo("Extraction", "No valid search rules were parsed."); return [], {}
    if not json_data: status_callback("No JSON data loaded for extraction."); return [], {}
    status_callback(f"Starting extraction with {len(parsed_rules)} rule(s)...")
    extracted_entries, rule_match_counts = [], {rule.raw_input: 0 for rule in parsed_rules} # Initialize counts
    _recursive_extraction(json_data, "", parsed_rules, extracted_entries, rule_match_counts)
    status_callback(f"Extraction core logic complete. Found {len(extracted_entries)} entries.")
    return extracted_entries, rule_match_counts


# --- Main Tkinter Application ---
class LocalizationSuiteApp:
    def __init__(self, master):
        self.master = master
        master.title("Comprehensive Localization Suite v1.4") # Version bump
        master.geometry("950x750")
        self.style = ttk.Style()
        try: # Set theme
            available_themes = self.style.theme_names()
            for theme in ['clam', 'alt', 'default', 'vista', 'xpnative']:
                if theme in available_themes: self.style.theme_use(theme); break
        except tk.TclError: pass

        default_file = "C:/Users/jorda/PycharmProjects/zxsjlocpipe/chinese_english_map.json"
        self.main_input_filepath = tk.StringVar(value=default_file)
        self.last_browsed_dir = os.path.expanduser("~")
        self._update_last_browsed_dir_from_main_file()

        # --- UI Structure (PanedWindow, File Input, Notebook, Status Log) ---
        self.main_paned_window = ttk.PanedWindow(master, orient=tk.VERTICAL)
        self.main_paned_window.pack(fill=tk.BOTH, expand=True)
        self.top_frame = ttk.Frame(self.main_paned_window, padding=5)
        self.main_paned_window.add(self.top_frame, weight=5)
        file_input_frame = ttk.LabelFrame(self.top_frame, text="Main JSON File", padding=5)
        file_input_frame.pack(fill=tk.X, padx=5, pady=5)
        ttk.Label(file_input_frame, text="Path:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.main_file_entry = ttk.Entry(file_input_frame, textvariable=self.main_input_filepath, width=80)
        self.main_file_entry.grid(row=0, column=1, padx=5, pady=5, sticky=tk.EW)
        self.main_file_browse_btn = ttk.Button(file_input_frame, text="Browse...", command=self.browse_main_input_file)
        self.main_file_browse_btn.grid(row=0, column=2, padx=5, pady=5)
        file_input_frame.columnconfigure(1, weight=1)
        self.notebook = ttk.Notebook(self.top_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self._setup_utilities_tab()
        self._setup_qa_tab()
        self._setup_merge_external_tab()
        self._setup_extraction_tab()
        status_frame = ttk.LabelFrame(self.main_paned_window, text="Status Log", padding=5)
        self.main_paned_window.add(status_frame, weight=1)
        self.status_text = scrolledtext.ScrolledText(status_frame, height=10, wrap=tk.WORD, state=tk.DISABLED, font=("Consolas", 9) if sys.platform == "win32" else ("Monaco", 10))
        self.status_text.pack(fill=tk.BOTH, expand=True)
        self.update_status("Localization Suite Ready.")
        if os.path.isfile(default_file): self.update_status(f"Default main file loaded: {default_file}")
        else: self.update_status(f"Default main file path set. Please verify or select a file.")

    def _update_last_browsed_dir_from_main_file(self):
        current_main_file = self.main_input_filepath.get()
        if current_main_file and os.path.isfile(current_main_file):
            self.last_browsed_dir = os.path.dirname(current_main_file)
        elif current_main_file and os.path.isdir(current_main_file): # If path is a dir
            self.last_browsed_dir = current_main_file


    def _get_initial_dir_for_dialog(self):
        current_main_file = self.main_input_filepath.get()
        if current_main_file and os.path.isfile(current_main_file):
            return os.path.dirname(current_main_file)
        # Check if last_browsed_dir is still valid, else default to user's home
        if os.path.isdir(self.last_browsed_dir):
            return self.last_browsed_dir
        return os.path.expanduser("~")


    def _update_status_text(self, message): # Runs in main thread via 'after'
        if not self.master.winfo_exists(): return
        self.status_text.config(state=tk.NORMAL)
        self.status_text.insert(tk.END, f"{time.strftime('%H:%M:%S')} - {message}\n")
        self.status_text.see(tk.END)
        self.status_text.config(state=tk.DISABLED)
        self.master.update_idletasks()

    def update_status(self, message): # Public method to call
        if self.master.winfo_exists(): self.master.after(0, self._update_status_text, message)

    def _set_buttons_state_recursive(self, widget, state):
        if isinstance(widget, (ttk.Button, ttk.Radiobutton, ttk.Checkbutton, ttk.Entry, scrolledtext.ScrolledText, tk.Text)): # Added tk.Text
            try:
                # scrolledtext.ScrolledText might not directly have 'state' for its text widget part in ttk version
                if isinstance(widget, scrolledtext.ScrolledText):
                     widget.configure(state=state if state == tk.NORMAL else tk.DISABLED) # For the underlying Text widget
                else:
                    widget.configure(state=state)
            except tk.TclError: pass
        for child in widget.winfo_children(): self._set_buttons_state_recursive(child, state)

    def set_ui_state(self, state):
        for tab_id in self.notebook.tabs():
            if self.master.winfo_exists():
                try:
                    # Get the actual frame widget for the tab
                    tab_widget = self.master.nametowidget(self.notebook.select()) # Get current tab
                    if tab_widget: # Ensure widget exists
                         self._set_buttons_state_recursive(tab_widget, state)
                    # Or iterate through all tabs if necessary, but disabling current is often enough
                    # for tab_name in self.notebook.tabs():
                    #    tab_frame = self.notebook.nametowidget(tab_name)
                    #    self._set_buttons_state_recursive(tab_frame, state)

                except tk.TclError as e:
                    # self.update_status(f"Minor UI state error: {e}") # Optional: for debugging
                    pass # Ignore if widget not found, might be during shutdown

        # Explicitly handle main file browse button and entry
        if self.master.winfo_exists():
            try:
                self.main_file_browse_btn.config(state=tk.NORMAL if state == tk.NORMAL else tk.DISABLED)
                self.main_file_entry.config(state=tk.NORMAL if state == tk.NORMAL else tk.DISABLED) # Also disable entry
            except tk.TclError:
                pass


    def browse_main_input_file(self):
        filepath = filedialog.askopenfilename(title="Select Main JSON File", filetypes=[("JSON files", "*.json"), ("All files", "*.*")], initialdir=self._get_initial_dir_for_dialog())
        if filepath: self.main_input_filepath.set(filepath); self.last_browsed_dir = os.path.dirname(filepath); self.update_status(f"Main input file set: {filepath}")


    def _validate_main_input(self):
        input_p = self.main_input_filepath.get()
        if not input_p: messagebox.showerror("Error", "Please select a Main JSON File first."); return None
        if not os.path.isfile(input_p): messagebox.showerror("Error", f"Main JSON File not found:\n{input_p}"); return None
        try:
            with open(input_p, 'r', encoding='utf-8') as f: data = json.load(f)
            self.update_status(f"Main JSON file '{os.path.basename(input_p)}' loaded successfully.")
            self._update_last_browsed_dir_from_main_file()
            return data
        except json.JSONDecodeError as e: self.update_status(f"JSON Decode Error in Main File: {e}"); messagebox.showerror("JSON Error", f"Invalid JSON in Main File:\n{input_p}\n\n{e}"); return None
        except Exception as e: self.update_status(f"Error reading Main File: {e}"); messagebox.showerror("File Error", f"Could not read Main File:\n{input_p}\n\n{e}"); return None


    def _setup_utilities_tab(self):
        tab_util = ttk.Frame(self.notebook, padding="10"); self.notebook.add(tab_util, text='File Utilities')
        split_frame = ttk.LabelFrame(tab_util, text="Split JSON", padding=10); split_frame.pack(fill=tk.X, pady=5, expand=True)
        ttk.Label(split_frame, text="Output Dir for Parts:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        self.split_output_dir = tk.StringVar(); self.split_output_dir_entry = ttk.Entry(split_frame, textvariable=self.split_output_dir, width=50)
        self.split_output_dir_entry.grid(row=0, column=1, sticky=tk.EW, padx=5, pady=2)
        ttk.Button(split_frame, text="Browse...", command=lambda: self._browse_directory(self.split_output_dir, title="Select Output Directory for Split Parts")).grid(row=0, column=2, padx=5, pady=2)
        ttk.Label(split_frame, text="Max Size (KB) per Part:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=2)
        self.split_max_size_kb = tk.StringVar(value="3000"); self.split_max_size_entry = ttk.Entry(split_frame, textvariable=self.split_max_size_kb, width=10)
        self.split_max_size_entry.grid(row=1, column=1, sticky=tk.W, padx=5, pady=2)
        self.split_button = ttk.Button(split_frame, text="Split Main File", command=self.run_split_json); self.split_button.grid(row=2, column=0, columnspan=3, pady=10)
        split_frame.columnconfigure(1, weight=1)
        merge_parts_frame = ttk.LabelFrame(tab_util, text="Merge JSON Parts", padding=10); merge_parts_frame.pack(fill=tk.X, pady=5, expand=True)
        ttk.Label(merge_parts_frame, text="Input Dir of Parts:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        self.merge_input_dir = tk.StringVar(); self.merge_input_dir_entry = ttk.Entry(merge_parts_frame, textvariable=self.merge_input_dir, width=50)
        self.merge_input_dir_entry.grid(row=0, column=1, sticky=tk.EW, padx=5, pady=2)
        ttk.Button(merge_parts_frame, text="Browse...", command=lambda: self._browse_directory(self.merge_input_dir, title="Select Directory of JSON Parts")).grid(row=0, column=2, padx=5, pady=2)
        ttk.Label(merge_parts_frame, text="Output Merged File:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=2)
        self.merge_output_file = tk.StringVar(); self.merge_output_file_entry = ttk.Entry(merge_parts_frame, textvariable=self.merge_output_file, width=50)
        self.merge_output_file_entry.grid(row=1, column=1, sticky=tk.EW, padx=5, pady=2)
        ttk.Button(merge_parts_frame, text="Save As...", command=lambda: self._browse_save_as(self.merge_output_file, title="Save Merged File As")).grid(row=1, column=2, padx=5, pady=2)
        self.merge_parts_button = ttk.Button(merge_parts_frame, text="Merge Parts", command=self.run_merge_json_parts); self.merge_parts_button.grid(row=2, column=0, columnspan=3, pady=10)
        merge_parts_frame.columnconfigure(1, weight=1)


    def _setup_qa_tab(self):
        tab_qa = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(tab_qa, text='Quality Checks')
        qa_config_frame = ttk.LabelFrame(tab_qa, text="Checks to Perform", padding=10)
        qa_config_frame.pack(fill=tk.X, pady=5)
        self.qa_check_chinese = tk.BooleanVar(value=True)
        self.qa_check_tags = tk.BooleanVar(value=True)
        self.qa_check_conditional = tk.BooleanVar(value=True)
        ttk.Checkbutton(qa_config_frame, text="Detect Chinese Characters in Values", variable=self.qa_check_chinese).pack(anchor=tk.W)
        ttk.Checkbutton(qa_config_frame, text="Check Tag Mismatches (Key vs. Value)", variable=self.qa_check_tags).pack(anchor=tk.W)
        ttk.Checkbutton(qa_config_frame, text="Check Conditional Content Mismatches (Key vs. Value)", variable=self.qa_check_conditional).pack(anchor=tk.W)
        self.run_qa_button = ttk.Button(qa_config_frame, text="Run Quality Checks & Auto-Export Issues", command=self.run_quality_checks)
        self.run_qa_button.pack(pady=10)

        qa_results_frame_text = "Issues Found (Will be auto-exported to 'issues_for_retranslation.json' in main file's directory if any are found)"
        qa_results_frame = ttk.LabelFrame(tab_qa, text=qa_results_frame_text, padding=10)
        qa_results_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        columns = ("Path", "Key", "Value", "Issue", "Details")
        self.qa_tree = ttk.Treeview(qa_results_frame, columns=columns, show="headings", selectmode="browse")
        for col in columns:
            self.qa_tree.heading(col, text=col)
            self.qa_tree.column(col, width=150, minwidth=100, stretch=tk.YES)
        self.qa_tree.column("Path", width=200, stretch=tk.YES)
        self.qa_tree.column("Key", width=200, stretch=tk.YES)
        self.qa_tree.column("Value", width=250, stretch=tk.YES)
        self.qa_tree.column("Details", width=250, stretch=tk.YES)
        qa_scrollbar_y = ttk.Scrollbar(qa_results_frame, orient=tk.VERTICAL, command=self.qa_tree.yview)
        qa_scrollbar_x = ttk.Scrollbar(qa_results_frame, orient=tk.HORIZONTAL, command=self.qa_tree.xview)
        self.qa_tree.configure(yscrollcommand=qa_scrollbar_y.set, xscrollcommand=qa_scrollbar_x.set)
        qa_scrollbar_y.pack(side=tk.RIGHT, fill=tk.Y)
        qa_scrollbar_x.pack(side=tk.BOTTOM, fill=tk.X)
        self.qa_tree.pack(fill=tk.BOTH, expand=True)
        self.qa_issues_data = [] # Store issues here

    def _setup_merge_external_tab(self):
        tab_merge_ext = ttk.Frame(self.notebook, padding="10"); self.notebook.add(tab_merge_ext, text='Merge External JSON')
        merge_ext_frame = ttk.LabelFrame(tab_merge_ext, text="Merge External File into Main File", padding=10); merge_ext_frame.pack(fill=tk.X, pady=5, expand=True)
        ttk.Label(merge_ext_frame, text="External JSON to Merge:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.merge_ext_filepath = tk.StringVar(); self.merge_ext_file_entry = ttk.Entry(merge_ext_frame, textvariable=self.merge_ext_filepath, width=60)
        self.merge_ext_file_entry.grid(row=0, column=1, padx=5, pady=5, sticky=tk.EW)
        ttk.Button(merge_ext_frame, text="Browse...", command=self.browse_external_merge_file).grid(row=0, column=2, padx=5, pady=5)
        merge_ext_frame.columnconfigure(1, weight=1)
        self.run_merge_ext_button = ttk.Button(merge_ext_frame, text="Merge Files & Save Result As...", command=self.run_merge_external_json)
        self.run_merge_ext_button.grid(row=1, column=0, columnspan=3, pady=10)
        ttk.Label(merge_ext_frame, text="NOTE: Keys from the external file will overwrite matching keys in the main file.\nDictionaries will be merged recursively. The merged result will prompt for a save location.").grid(row=2, column=0, columnspan=3, padx=5, pady=5, sticky=tk.W)

    def _setup_extraction_tab(self):
        tab_extract = ttk.Frame(self.notebook, padding="10"); self.notebook.add(tab_extract, text='Data Extraction')
        extract_config_frame = ttk.LabelFrame(tab_extract, text="Extraction Rules", padding=10); extract_config_frame.pack(fill=tk.X, pady=5)
        ttk.Label(extract_config_frame, text="Search Criteria (one per line; # for comments):\n- 'KeySubstring' (extract if key contains this)\n- 'KeySubstring,ValueExclusionSubstring' (extract if key contains AND value DOES NOT contain exclusion)").pack(anchor=tk.W, pady=(0,5))
        self.extract_rules_text = scrolledtext.ScrolledText(extract_config_frame, height=5, width=70, wrap=tk.WORD)
        self.extract_rules_text.pack(fill=tk.X, expand=True, pady=5); self.extract_rules_text.insert(tk.END, "# Example: texts\n# Example: ui_string,UNUSED_STRING")
        self.run_extraction_button = ttk.Button(extract_config_frame, text="Extract from Main File", command=self.run_substring_extraction); self.run_extraction_button.pack(pady=10)
        extract_results_frame = ttk.LabelFrame(tab_extract, text="Extracted Entries", padding=10); extract_results_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        columns = ("Path", "Key", "Value"); self.extract_tree = ttk.Treeview(extract_results_frame, columns=columns, show="headings", selectmode="browse")
        for col in columns: self.extract_tree.heading(col, text=col); self.extract_tree.column(col, width=200, minwidth=150, stretch=tk.YES)
        self.extract_tree.column("Value", width=300, stretch=tk.YES)
        extract_scrollbar_y = ttk.Scrollbar(extract_results_frame, orient=tk.VERTICAL, command=self.extract_tree.yview)
        extract_scrollbar_x = ttk.Scrollbar(extract_results_frame, orient=tk.HORIZONTAL, command=self.extract_tree.xview)
        self.extract_tree.configure(yscrollcommand=extract_scrollbar_y.set, xscrollcommand=extract_scrollbar_x.set)
        extract_scrollbar_y.pack(side=tk.RIGHT, fill=tk.Y); extract_scrollbar_x.pack(side=tk.BOTTOM, fill=tk.X); self.extract_tree.pack(fill=tk.BOTH, expand=True)
        self.export_extract_button = ttk.Button(extract_results_frame, text="Export Extractions to JSON...", command=self.export_extracted_data, state=tk.DISABLED)
        self.export_extract_button.pack(pady=5, side=tk.LEFT)
        self.extracted_data_cache = []; self.extraction_rule_counts = {}


    def _browse_directory(self, string_var, title="Select Directory"):
        dirpath = filedialog.askdirectory(title=title, initialdir=self._get_initial_dir_for_dialog())
        if dirpath: string_var.set(dirpath); self.last_browsed_dir = dirpath

    def _browse_save_as(self, string_var_to_set, title="Save File As", defaultextension=".json", filetypes=[("JSON files", "*.json")], initialfile_name=None): # MODIFIED
        filepath = filedialog.asksaveasfilename(
            title=title,
            defaultextension=defaultextension,
            filetypes=filetypes + [("All files", "*.*")],
            initialdir=self._get_initial_dir_for_dialog(),
            initialfile=initialfile_name # Use the new parameter
        )
        if filepath:
            if string_var_to_set: string_var_to_set.set(filepath)
            self.last_browsed_dir = os.path.dirname(filepath)
            return filepath
        return None

    def browse_external_merge_file(self):
        filepath = filedialog.askopenfilename(title="Select External JSON to Merge", filetypes=[("JSON files", "*.json"), ("All files", "*.*")], initialdir=self._get_initial_dir_for_dialog())
        if filepath: self.merge_ext_filepath.set(filepath); self.last_browsed_dir = os.path.dirname(filepath); self.update_status(f"External merge file set: {filepath}")

    def _run_threaded_action(self, action_func, *args):
        self.set_ui_state(tk.DISABLED)
        self.update_status(f"Starting '{action_func.__name__}'...")
        thread = threading.Thread(target=self._action_wrapper, args=(action_func, *args), daemon=True); thread.start()


    def _action_wrapper(self, action_func, *args):
        if self.master.winfo_exists():
            self.master.after(10, lambda: [self.status_text.config(state=tk.NORMAL), self.status_text.delete('1.0', tk.END), self.status_text.config(state=tk.DISABLED), self._update_status_text(f"Executing '{action_func.__name__}' in background...")])
        success = False; action_name = action_func.__name__
        try:
            time.sleep(0.05)
            success = action_func(*args)
        except Exception as e:
            self.update_status(f"FATAL ERROR in {action_name}: {e}"); import traceback; self.update_status(traceback.format_exc())
            if self.master.winfo_exists(): self.master.after(0, lambda e=e, an=action_name: messagebox.showerror("Fatal Error", f"An unexpected error occurred in {an}:\n{e}"))
        finally:
            if self.master.winfo_exists():
                self.master.after(0, lambda: self.set_ui_state(tk.NORMAL))
                final_msg = "completed successfully." if success else "finished with errors or was cancelled."
                self.update_status(f"Action '{action_name}' {final_msg}")

    def run_split_json(self):
        input_p = self.main_input_filepath.get();
        if not self._validate_main_input(): return
        out_dir = self.split_output_dir.get()
        if not out_dir: messagebox.showerror("Error", "Please specify an Output Directory for split parts."); return
        try: max_kb = int(self.split_max_size_kb.get()); assert max_kb > 0
        except (ValueError, AssertionError): messagebox.showerror("Error", "Invalid Max Size (KB). Must be a positive integer."); return
        self._run_threaded_action(do_split_json, input_p, out_dir, max_kb, self.update_status)


    def run_merge_json_parts(self):
        in_dir = self.merge_input_dir.get()
        if not in_dir or not os.path.isdir(in_dir): messagebox.showerror("Error", "Please select a valid Input Directory for JSON parts."); return
        if not self.merge_output_file.get():
            self.update_status("Please specify the output merged file path using 'Save As...' for merge operation.")
            if not self._browse_save_as(self.merge_output_file, title="Save Merged File As", initialfile_name="merged_output.json"): # Provide a default name
                self.update_status("Merge parts cancelled: No output file selected."); return
        final_out_file = self.merge_output_file.get()
        if not final_out_file: self.update_status("Merge parts cancelled: Output file path is empty."); return
        self._run_threaded_action(do_merge_json_parts, in_dir, final_out_file, self.update_status)


    def _auto_export_qa_issues(self): # Uses self.qa_issues_data
        if not self.master.winfo_exists(): return False

        main_file_path = self.main_input_filepath.get()
        if not main_file_path or not os.path.isfile(main_file_path):
            self.update_status("Error: Cannot auto-export QA issues, main input file path is invalid.")
            # messagebox.showerror("Auto-Export Error", "Main input file path is invalid. Cannot determine export location.") # Already in thread
            return False

        output_dir = os.path.dirname(main_file_path)
        output_filename = "issues_for_retranslation.json"
        full_output_path = os.path.join(output_dir, output_filename)

        export_structure = {}
        unique_keys_for_export = {}

        for issue in self.qa_issues_data:
            full_path_to_key = issue["path"]
            # json_key_to_translate = issue["key"] # Original JSON key at that path
            value_from_original_json = issue["value"] # Value from original JSON at that path
            if full_path_to_key not in unique_keys_for_export:
                unique_keys_for_export[full_path_to_key] = value_from_original_json

        if not unique_keys_for_export:
            self.update_status("QA: No unique issues found to auto-export.")
            return True # Still considered a success in terms of the process

        for full_path_to_key, value_to_export in unique_keys_for_export.items():
            path_segments = full_path_to_key.split('.')
            actual_json_key_segment = path_segments[-1]
            current_level_dict = export_structure
            for segment in path_segments[:-1]:
                current_level_dict = current_level_dict.setdefault(segment, {})
            current_level_dict[actual_json_key_segment] = value_to_export

        self.update_status(f"Auto-exporting {len(unique_keys_for_export)} unique problematic entries to {full_output_path}...")
        try:
            with open(full_output_path, 'w', encoding='utf-8') as f:
                json.dump(export_structure, f, ensure_ascii=False, indent=4)
            self.update_status(f"QA Issues auto-exported successfully to {full_output_path}")
            if unique_keys_for_export and self.master.winfo_exists(): # Show messagebox from main thread
                 self.master.after(0, lambda: messagebox.showinfo("Auto-Export Successful", f"{len(unique_keys_for_export)} unique issue entries automatically exported to:\n{full_output_path}"))
            return True
        except Exception as e:
            self.update_status(f"Error auto-exporting QA issues: {e}")
            import traceback; self.update_status(traceback.format_exc())
            # messagebox.showerror("Auto-Export Error", f"Could not auto-export issues to {full_output_path}:\n{e}") # Already in thread
            return False

    def run_quality_checks(self):
        json_data = self._validate_main_input()
        if json_data is None: return
        checks_config = {
            "chinese": self.qa_check_chinese.get(),
            "tags": self.qa_check_tags.get(),
            "conditional": self.qa_check_conditional.get()
        }
        if not any(checks_config.values()):
            messagebox.showinfo("Quality Checks", "No checks selected.")
            self.update_status("No quality checks selected to run.")
            return

        if self.master.winfo_exists():
            for i in self.qa_tree.get_children(): self.qa_tree.delete(i)
        self.qa_issues_data = []

        def perform_checks_and_auto_export(): # This runs in the thread
            issues = do_quality_checks(json_data, checks_config, self.update_status)
            self.qa_issues_data = issues

            if self.qa_issues_data:
                self._auto_export_qa_issues() # Uses self.qa_issues_data

            def _update_gui_after_checks(): # Scheduled for main thread
                if not self.master.winfo_exists(): return
                for issue_item in self.qa_issues_data:
                    self.qa_tree.insert("", tk.END, values=(
                        issue_item.get("path", ""), issue_item.get("key", ""), issue_item.get("value", ""),
                        issue_item.get("issue", ""), issue_item.get("details", "")
                    ))
                if self.qa_issues_data:
                    self.update_status(f"QA checks displayed. {len(self.qa_issues_data)} issues found.")
                else:
                    self.update_status("QA checks complete. No issues found.")

            if self.master.winfo_exists(): self.master.after(0, _update_gui_after_checks)
            return True # Overall action success

        self._run_threaded_action(perform_checks_and_auto_export)

    def run_merge_external_json(self):
        main_p = self.main_input_filepath.get()
        if not main_p or not os.path.isfile(main_p): messagebox.showerror("Error", "Please select a valid Main JSON File first."); return
        ext_p = self.merge_ext_filepath.get()
        if not ext_p or not os.path.isfile(ext_p): messagebox.showerror("Error", "Please select a valid External JSON file to merge."); return
        if not messagebox.askyesno("Confirm Merge", f"This will merge '{os.path.basename(ext_p)}' into '{os.path.basename(main_p)}'.\nMatching keys will be overwritten (dictionaries merged deeply).\nYou will be prompted to save the result AS A NEW FILE or OVERWRITE.\n\nProceed with merge?"):
            self.update_status("External JSON merge cancelled by user."); return
        self._run_threaded_action(do_merge_external_json, main_p, ext_p, self.update_status)


    def run_substring_extraction(self):
        json_data = self._validate_main_input()
        if json_data is None: return
        rules_text = self.extract_rules_text.get("1.0", tk.END).strip()
        if not rules_text:
            messagebox.showinfo("Extraction", "Please enter extraction rules.")
            self.update_status("No extraction rules entered.")
            return
        
        # Clear previous results from cache before starting new extraction
        self.extracted_data_cache = []
        self.extraction_rule_counts = {}
        # TreeView and export button are handled in _finalize_extraction_in_main_thread

        def perform_extraction_and_update_gui(): # Runs in worker thread
            # Perform the core data extraction
            entries, rule_counts = do_substring_extraction(json_data, rules_text, self.update_status)
            
            # Store results for the main thread
            self.extracted_data_cache = entries
            self.extraction_rule_counts = rule_counts

            # This nested function will be scheduled to run in the main thread
            def _finalize_extraction_in_main_thread():
                if not self.master.winfo_exists(): return # Check if UI is still there

                # 1. Clear previous entries from the TreeView
                for i in self.extract_tree.get_children():
                    self.extract_tree.delete(i)

                # 2. Populate the TreeView with new entries
                for entry in self.extracted_data_cache:
                    self.extract_tree.insert("", tk.END, values=(
                        entry.get("path", ""),
                        entry.get("key", ""),
                        entry.get("value", "")
                    ))

                # 3. Update status messages and export button state
                if self.extracted_data_cache:
                    self.export_extract_button.config(state=tk.NORMAL) # Enable manual export button
                    self.update_status(f"Extraction displayed. {len(self.extracted_data_cache)} entries found. Triggering auto-export...")
                    
                    # 4. --- AUTO-EXPORT ---
                    # This call will now happen automatically if data is found.
                    # self.export_extracted_data() handles its own dialogs and runs in the main thread.
                    self.export_extracted_data() 
                else:
                    self.export_extract_button.config(state=tk.DISABLED)
                    self.update_status("Extraction complete. No matching entries found.")

                # 5. Display rule match counts
                if self.extraction_rule_counts:
                    self.update_status("Extraction counts per rule:")
                    for rule, count in sorted(self.extraction_rule_counts.items()):
                        self.update_status(f"   - '{rule}': {count}")
            
            # Schedule the UI updates and auto-export to run in the main thread
            if self.master.winfo_exists():
                self.master.after(0, _finalize_extraction_in_main_thread)
            
            return True # Indicate success of the threaded part of the action

        # Start the threaded action
        self._run_threaded_action(perform_extraction_and_update_gui)

    def export_extracted_data(self):
        if not self.extracted_data_cache:
            messagebox.showinfo("Export Extractions", "No data extracted to export. Please run extraction first.")
            self.update_status("No extracted data available for export.")
            return

        filepath_var = tk.StringVar()
        default_filename = "extracted_data.json"
        main_input_file_path = self.main_input_filepath.get()
        if main_input_file_path and os.path.isfile(main_input_file_path):
            base = os.path.splitext(os.path.basename(main_input_file_path))[0]
            default_filename = f"{base}_extracted_data.json"
        
        actual_filepath_to_save = self._browse_save_as(
            filepath_var, 
            title="Export Extracted Data As JSON (Structured)",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json")],
            initialfile_name=default_filename
        )

        if not actual_filepath_to_save:
            self.update_status("Extraction export cancelled.")
            return

        structured_export_data = {}
        # Use a dictionary to ensure each unique original path is processed once,
        # taking the value associated with it.
        unique_items_by_original_path = {} 
        for item in self.extracted_data_cache: # self.extracted_data_cache is list of {"path": ..., "key": ..., "value": ...}
            path_str = item.get("path")
            if path_str: # Ensure path is not None or empty
                unique_items_by_original_path[path_str] = item.get("value") # Last seen value for a given path wins

        if not unique_items_by_original_path:
            self.update_status("No valid entries with paths found in extracted data to format and export.")
            messagebox.showinfo("Export Extractions", "No valid entries found after initial processing to export.")
            return

        processed_items_count = 0
        skipped_items_log = []

        for original_full_path, item_value in unique_items_by_original_path.items():
            path_segments = original_full_path.split('.')

            if not path_segments: # Should be rare if original_full_path was valid
                skipped_items_log.append(f"Skipping entry with malformed (empty) path segments from: {original_full_path}")
                continue

            top_level_namespace_key = path_segments[0]
            inner_dict_key: str

            if len(path_segments) == 1:
                # Path is like "MyKey". Top namespace is "MyKey", inner key is also "MyKey".
                inner_dict_key = path_segments[0] 
            else: # len(path_segments) > 1, path is like "Namespace.Sub.Key"
                # Inner key becomes "Sub.Key"
                inner_dict_key = ".".join(path_segments[1:])
            
            if not inner_dict_key: # Should not happen if path_segments was not empty. Defensive.
                 skipped_items_log.append(f"Failed to derive an inner dictionary key for path: {original_full_path}")
                 continue

            # Ensure the top-level namespace key maps to a dictionary in the output
            if top_level_namespace_key not in structured_export_data:
                structured_export_data[top_level_namespace_key] = {}
            elif not isinstance(structured_export_data[top_level_namespace_key], dict):
                skipped_items_log.append(
                    f"Conflict: Key '{top_level_namespace_key}' already exists as a non-dictionary value. "
                    f"Cannot create as a namespace for path '{original_full_path}'. Skipping this item."
                )
                continue # Skip this item, cannot create the required structure

            # Get the target inner dictionary
            inner_dictionary = structured_export_data[top_level_namespace_key]
            
            # Check for overwrite in the inner dictionary.
            # This means two different original_full_path entries resolved to the
            # same top_level_namespace_key AND the same inner_dict_key.
            if inner_dict_key in inner_dictionary and inner_dictionary[inner_dict_key] != item_value:
                 skipped_items_log.append(
                    f"Warning: Value for key '{inner_dict_key}' in namespace '{top_level_namespace_key}' is being overwritten. "
                    f"Original path for this entry: {original_full_path}. Old value was '{inner_dictionary[inner_dict_key]}', New value: '{item_value}'."
                )
            
            inner_dictionary[inner_dict_key] = item_value
            processed_items_count += 1

        # Log any items that were skipped or caused issues during structuring
        for log_entry in skipped_items_log:
            self.update_status(f"Warning (Export Structuring): {log_entry}")

        final_output_message: str
        if not structured_export_data and unique_items_by_original_path:
            final_output_message = "Export resulted in an empty structure, possibly due to conflicts for all items. Check status log."
            self.update_status(final_output_message)
            messagebox.showwarning("Export Issue", final_output_message)
        elif processed_items_count == 0 and unique_items_by_original_path: # No items processed but there were unique paths
            final_output_message = "No items were successfully structured for export. Check status log for conflicts."
            self.update_status(final_output_message)
            messagebox.showwarning("Export Issue", final_output_message)
        # else: proceed to save

        self.update_status(f"Structuring complete. Attempting to export {processed_items_count} processed key-value pairs (from {len(unique_items_by_original_path)} unique extracted paths) to {actual_filepath_to_save}...")
        try:
            with open(actual_filepath_to_save, 'w', encoding='utf-8') as f:
                json.dump(structured_export_data, f, ensure_ascii=False, indent=4)
            self.update_status(f"Structured extracted data exported successfully to {actual_filepath_to_save}")
            
            summary_message = (
                f"Data based on {processed_items_count} key-value pairs (from {len(unique_items_by_original_path)} unique "
                f"extracted paths) has been structured and exported to:\n{actual_filepath_to_save}"
            )
            if skipped_items_log: # Use len(skipped_items_log) for clarity
                 summary_message += f"\n\nNote: {len(skipped_items_log)} issues (e.g. conflicts, skips) occurred during structuring. See status log for details."
            messagebox.showinfo("Export Successful", summary_message)

        except Exception as e:
            self.update_status(f"Error exporting structured extracted data: {e}")
            import traceback
            self.update_status(traceback.format_exc())
            messagebox.showerror("Export Error", f"Could not export structured data:\n{e}")


# --- Main Execution ---
if __name__ == "__main__":
    root = tk.Tk()
    app = LocalizationSuiteApp(root)
    root.mainloop()