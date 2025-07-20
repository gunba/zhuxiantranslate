import json
import os
import re # For checking Chinese/Cyrillic characters AND for new normalization
from collections import defaultdict
import pandas as pd # For reading Excel
import multiprocessing # For true multithreading
from tqdm import tqdm # For progress bar
import opencc # For Simplified/Traditional Chinese conversion
import ahocorasick # NEW: For high-performance rule pre-filtering

# --- Configuration (remains the same) ---
UNIFIED_JSON_INPUT_PATH = "./unified_zxsj_data.json"
TRANSLATION_MAP_PATH = "./chinese_english_map.json"
TRANSLATED_JSON_OUTPUT_PATH = "./translated_unified_zxsj_data.json"
UNTRANSLATED_EXCERPT_PATH = "./untranslated_chinese_strings.json"
NORMALISED_MAP_EXCEL_PATH = "./normalised_map.xlsx"
NORMALISED_MAP_SHEET_NAME = "normalised_map"
# NEW: Path for the rule tracking output
RULE_TRACKING_OUTPUT_PATH = "./rule_application_tracking.json"

SESSION_CROSS_NAMESPACE_TRANSLATIONS_CACHE = {}

# REMOVED: Global rule list for workers is no longer needed
# worker_rules_list_global = None

# Initialize OpenCC converters
s2t_converter = opencc.OpenCC('s2t')  # Simplified to Traditional
t2s_converter = opencc.OpenCC('t2s')  # Traditional to Simplified

# NEW: Caches for performance
PATTERN_CACHE = {}  # Cache for number pattern extraction
CONVERSION_CACHE = {}  # Cache for OpenCC conversions
TRANSLATION_MAP_INDEX = None  # Pre-computed index for faster lookups

# REMOVED: Worker initializer is no longer needed
# def init_worker_rules(rules_list_arg):
#     global worker_rules_list_global
#     worker_rules_list_global = rules_list_arg

# --- MODIFIED HELPER FUNCTION ---
def contains_cn_or_ru(text):
    """
    Checks if the given text string contains Chinese or Cyrillic characters.
    """
    if not isinstance(text, str): return False
    # Regex for Chinese characters (existing)
    chinese_regex = r'[\u4e00-\u9fff\u3400-\u4dbf\uf900-\ufaff\U00020000-\U0002A6DF\U0002A700-\U0002B73F\U0002B740-\U0002B81F\U0002B820-\U0002CEAF\U0002CEB0-\U0002EBEF\U0002EBF0-\U0002EE5F\U00030000-\U0003134F\U00031350-\U000323AF\U0002F800-\U0002FA1F]'
    # Regex for Cyrillic characters (added)
    cyrillic_regex = r'[\u0400-\u04FF\u0500-\u052F\u2DE0-\u2DFF\uA640-\uA69F]'
    
    # Check if either Chinese or Cyrillic characters are present
    if re.search(chinese_regex, text) or re.search(cyrillic_regex, text):
        return True
    return False

def normalize_key(key_string):
    if isinstance(key_string, str):
        if key_string.startswith('\ufeff'): key_string = key_string[1:]
        return key_string.strip()
    return key_string

# --- MODIFIED FUNCTION ---
def normalize_text_for_pattern_key(text):
    """
    Normalizes text for creating robust matching keys.
    """
    if not isinstance(text, str):
        return text
    normalized_newlines = text.replace('\r\n', '\n').replace('\r', '\n')
    text_without_rtp_tag = normalized_newlines.replace('<RTP_Default></>', '')
    collapsed_newlines = re.sub(r'\n{2,}', '\n', text_without_rtp_tag)
    return collapsed_newlines.strip()

def extract_number_pattern(original_text):
    if not isinstance(original_text, str):
        return None, []
    if original_text in PATTERN_CACHE:
        return PATTERN_CACHE[original_text]
    
    text_for_pattern_key = normalize_text_for_pattern_key(original_text)
    number_regex_str = r'[\d]+(?:\.[\d]+)?%?'
    numbers = re.findall(number_regex_str, original_text)
    pattern_key_generated = re.sub(number_regex_str, '{}', text_for_pattern_key)
    
    result = (pattern_key_generated, numbers)
    PATTERN_CACHE[original_text] = result
    return result

def get_opencc_conversions(text):
    if text in CONVERSION_CACHE:
        return CONVERSION_CACHE[text]
    try:
        traditional = s2t_converter.convert(text)
        simplified = t2s_converter.convert(text)
        result = (traditional, simplified)
        CONVERSION_CACHE[text] = result
        return result
    except Exception:
        CONVERSION_CACHE[text] = (text, text)
        return (text, text)

def replace_numbers_in_translation(original_chinese, template_chinese, template_translation):
    if not all(isinstance(x, str) for x in [original_chinese, template_chinese, template_translation]):
        return None
    orig_pattern, orig_numbers = extract_number_pattern(original_chinese)
    template_pattern, template_numbers = extract_number_pattern(template_chinese)
    trans_pattern, trans_numbers = extract_number_pattern(template_translation)

    if orig_pattern != template_pattern:
        return None
    if len(orig_numbers) != len(template_numbers) or len(orig_numbers) != len(trans_numbers):
        return None
    
    result = template_translation
    for i, (old_num, new_num) in enumerate(zip(trans_numbers, orig_numbers)):
        result = re.sub(r'\b' + re.escape(old_num) + r'\b', new_num, result, count=1)
    return result

def build_translation_index(translation_map):
    index = {'exact': {}, 'pattern': defaultdict(list), 'converted': {}}
    print("Building translation index with advanced normalization for faster lookups...")
    for namespace, namespace_dict in translation_map.items():
        if not isinstance(namespace_dict, dict): continue
        for chinese_key, translation in namespace_dict.items():
            if not isinstance(translation, str) or not translation: continue
            
            normalized_cjk_for_exact_index = normalize_text_for_pattern_key(chinese_key)
            if normalized_cjk_for_exact_index not in index['exact']:
                index['exact'][normalized_cjk_for_exact_index] = translation
            
            pattern_key, numbers = extract_number_pattern(chinese_key)
            if pattern_key and numbers:
                index['pattern'][pattern_key].append((chinese_key, translation, namespace))
            
            traditional, simplified = get_opencc_conversions(chinese_key)
            # S/T logic can be further optimized if needed, but is not the main bottleneck
    print(f"Translation index built: {len(index['exact'])} exact, {len(index['pattern'])} patterns.")
    return index

def find_existing_translation_elsewhere(chinese_text_to_find, entire_translation_map):
    global TRANSLATION_MAP_INDEX
    if not isinstance(chinese_text_to_find, str) or not chinese_text_to_find:
        return None, None
    if chinese_text_to_find in SESSION_CROSS_NAMESPACE_TRANSLATIONS_CACHE:
        return SESSION_CROSS_NAMESPACE_TRANSLATIONS_CACHE[chinese_text_to_find]
    if TRANSLATION_MAP_INDEX is None:
        TRANSLATION_MAP_INDEX = build_translation_index(entire_translation_map)

    def _prepare_and_cache_result(translation_candidate, method_found, original_input_key_for_cache):
        SESSION_CROSS_NAMESPACE_TRANSLATIONS_CACHE[original_input_key_for_cache] = (translation_candidate, method_found)
        return translation_candidate, method_found

    normalized_input_for_exact = normalize_text_for_pattern_key(chinese_text_to_find)
    if normalized_input_for_exact in TRANSLATION_MAP_INDEX['exact']:
        translation = TRANSLATION_MAP_INDEX['exact'][normalized_input_for_exact]
        return _prepare_and_cache_result(translation, "exact_global_fully_normalized", chinese_text_to_find)

    input_pattern_key, input_numbers = extract_number_pattern(chinese_text_to_find)
    if input_pattern_key and input_numbers and input_pattern_key in TRANSLATION_MAP_INDEX['pattern']:
        for map_chinese_key, map_translation_template, _ in TRANSLATION_MAP_INDEX['pattern'][input_pattern_key]:
            adapted_translation = replace_numbers_in_translation(chinese_text_to_find, map_chinese_key, map_translation_template)
            if adapted_translation:
                return _prepare_and_cache_result(adapted_translation, "pattern_original_fully_normalized_pattern", chinese_text_to_find)

    traditional_input, simplified_input = get_opencc_conversions(chinese_text_to_find)
    for converted_variant_of_input in [traditional_input, simplified_input]:
        if converted_variant_of_input != chinese_text_to_find:
            normalized_converted_variant = normalize_text_for_pattern_key(converted_variant_of_input)
            if normalized_converted_variant in TRANSLATION_MAP_INDEX['exact']:
                translation = TRANSLATION_MAP_INDEX['exact'][normalized_converted_variant]
                return _prepare_and_cache_result(translation, "st_exact_fully_normalized", chinese_text_to_find)
            
            converted_input_pattern_key, _ = extract_number_pattern(converted_variant_of_input)
            if converted_input_pattern_key and input_numbers and converted_input_pattern_key in TRANSLATION_MAP_INDEX['pattern']:
                for map_chinese_key, map_translation_template, _ in TRANSLATION_MAP_INDEX['pattern'][converted_input_pattern_key]:
                    adapted_translation = replace_numbers_in_translation(chinese_text_to_find, map_chinese_key, map_translation_template)
                    if adapted_translation:
                        return _prepare_and_cache_result(adapted_translation, "st_pattern_fully_normalized_pattern", chinese_text_to_find)

    SESSION_CROSS_NAMESPACE_TRANSLATIONS_CACHE[chinese_text_to_find] = (None, None)
    return None, None

def translate_data_with_conditions(unified_data, translation_map_obj_as_is):
    translated_data = {}
    untranslated_excerpt = defaultdict(dict)
    stats = defaultdict(int)
    SESSION_CROSS_NAMESPACE_TRANSLATIONS_CACHE.clear()
    PATTERN_CACHE.clear()
    CONVERSION_CACHE.clear()
    global TRANSLATION_MAP_INDEX
    TRANSLATION_MAP_INDEX = None

    used_translation_map = {}

    for namespace_from_unified_data, key_value_pairs_from_unified_data in tqdm(unified_data.items(), desc="Translating namespaces"):
        translated_data[namespace_from_unified_data] = {}
        if not isinstance(key_value_pairs_from_unified_data, dict):
            translated_data[namespace_from_unified_data] = key_value_pairs_from_unified_data
            continue

        current_namespace_translation_dict = translation_map_obj_as_is.get(namespace_from_unified_data)

        for key_from_unified_data, value_to_translate in key_value_pairs_from_unified_data.items():
            final_value_for_output_json = value_to_translate
            if not isinstance(value_to_translate, str):
                translated_data[namespace_from_unified_data][key_from_unified_data] = final_value_for_output_json
                continue

            stats["total_strings_processed"] += 1
            if ".data" in value_to_translate or 'Texture2D' in value_to_translate:
                stats["skipped_contains_data_keyword"] += 1
                translated_data[namespace_from_unified_data][key_from_unified_data] = final_value_for_output_json
                continue
            if not contains_cn_or_ru(value_to_translate):
                stats["skipped_no_cn_or_ru_chars"] += 1
                translated_data[namespace_from_unified_data][key_from_unified_data] = final_value_for_output_json
                continue

            translation_found_for_this_string = False
            if current_namespace_translation_dict and isinstance(current_namespace_translation_dict, dict):
                english_translation = current_namespace_translation_dict.get(value_to_translate)
                if isinstance(english_translation, str) and english_translation:
                    final_value_for_output_json = english_translation
                    stats["translations_applied_from_map"] += 1
                    translation_found_for_this_string = True
                    if namespace_from_unified_data not in used_translation_map:
                        used_translation_map[namespace_from_unified_data] = {}
                    used_translation_map[namespace_from_unified_data][value_to_translate] = english_translation

            if not translation_found_for_this_string:
                retrieved_translation_str, method_found = find_existing_translation_elsewhere(value_to_translate, translation_map_obj_as_is)
                if retrieved_translation_str:
                    final_value_for_output_json = retrieved_translation_str
                    stats["new_translations_sourced_and_added"] += 1
                    translation_found_for_this_string = True
                    if namespace_from_unified_data not in used_translation_map:
                        used_translation_map[namespace_from_unified_data] = {}
                    used_translation_map[namespace_from_unified_data][value_to_translate] = retrieved_translation_str

            translated_data[namespace_from_unified_data][key_from_unified_data] = final_value_for_output_json
            if not translation_found_for_this_string:
                untranslated_excerpt[namespace_from_unified_data][value_to_translate] = ""
                stats["added_to_untranslated_excerpt"] += 1

    for namespace, translations_in_namespace in used_translation_map.items():
        if namespace not in translation_map_obj_as_is:
            translation_map_obj_as_is[namespace] = {}
        elif not isinstance(translation_map_obj_as_is[namespace], dict):
            translation_map_obj_as_is[namespace] = {}
        translation_map_obj_as_is[namespace].update(translations_in_namespace)

    print("\n--- Translation Summary ---")
    for stat_key, stat_value in stats.items(): print(f"{stat_key.replace('_', ' ').capitalize()}: {stat_value}")
    return translated_data, untranslated_excerpt

def normalize_dictionary_keys_recursively(data_to_normalize):
    if isinstance(data_to_normalize, dict):
        return {normalize_key(k): normalize_dictionary_keys_recursively(v) for k, v in data_to_normalize.items()}
    elif isinstance(data_to_normalize, list):
        return [normalize_dictionary_keys_recursively(item) for item in data_to_normalize]
    else:
        return data_to_normalize

def load_excel_rules(excel_path, sheet_name):
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name, dtype=str)
        required_cols = ['Simp Chinese', 'Trad Chinese', 'Good Translation', 'Bad Translation']
        for col in required_cols:
            if col not in df.columns:
                print(f"ERROR: Missing required column in Excel: {col}. Post-processing will be skipped.")
                return []

        str_cols_to_clean = ['Simp Chinese', 'Trad Chinese', 'Good Translation', 'Bad Translation']
        for col in str_cols_to_clean:
            if col in df.columns:
                df[col] = df[col].fillna('')
            else:
                df[col] = ''

        df['len_simp_chinese'] = df['Simp Chinese'].astype(str).map(len)
        df['len_bad_translation'] = df['Bad Translation'].astype(str).map(len)
        df.sort_values(by=['len_simp_chinese', 'len_bad_translation'], ascending=[False, False], inplace=True)
        
        rules_list = df.to_dict('records')
        for idx, rule in enumerate(rules_list):
            rule['__rule_id__'] = idx
        return rules_list
    except FileNotFoundError:
        print(f"ERROR: Excel rule file not found at '{excel_path}'. Post-processing will be skipped.")
        return []
    except Exception as e:
        print(f"ERROR: Could not load or process Excel rules from '{excel_path}': {e}. Post-processing will be skipped.")
        return []

# MODIFIED: Renamed function, no longer needs _mp suffix
# MODIFIED: Takes a filtered list of 'applicable_rules' instead of using a global
def execute_single_string_post_processing(original_chinese, current_english, debug_info, applicable_rules):
    rule_applications = []

    if not isinstance(current_english, str) or not applicable_rules:
        return current_english, rule_applications

    # The rules are already pre-filtered and sorted.
    all_rules_sorted = applicable_rules
    chars = list(current_english)

    persistent_active_locks = [False] * len(chars)
    active_good_translation_owners_map = defaultdict(list)
    
    # First loop: Build conflict map from the SMALL, pre-filtered list of rules
    for r_init in all_rules_sorted:
        cn_s_init = r_init.get('Simp Chinese', '')
        cn_t_init = r_init.get('Trad Chinese', '')
        is_r_init_active = False
        # This check is slightly redundant because of pre-filtering, but is kept for correctness
        if cn_s_init and cn_s_init in original_chinese: is_r_init_active = True
        if not is_r_init_active and cn_t_init and cn_t_init != cn_s_init and cn_t_init in original_chinese: is_r_init_active = True

        if is_r_init_active:
            good_t_init = r_init.get('Good Translation', '')
            if isinstance(good_t_init, str) and good_t_init:
                active_good_translation_owners_map[good_t_init].append(r_init['__rule_id__'])
    
    # Second loop: Apply replacements using the SMALL, pre-filtered list of rules
    for rule_data_current in all_rules_sorted:
        # ... (The rest of this function's logic is unchanged)
        rule_current_id = rule_data_current['__rule_id__']
        bad_en_str_current = rule_data_current.get('Bad Translation', '')
        good_en_str_current = rule_data_current.get('Good Translation', '')
        if not isinstance(good_en_str_current, str): good_en_str_current = ""

        if not (isinstance(bad_en_str_current, str) and bad_en_str_current):
            continue

        list_bad_en_current = list(bad_en_str_current)
        len_list_bad_en_current = len(list_bad_en_current)
        list_good_en_current = list(good_en_str_current)
        len_list_good_en_current = len(list_good_en_current)

        cn_simp_current = rule_data_current.get('Simp Chinese', '')
        cn_trad_current = rule_data_current.get('Trad Chinese', '')
        is_rule_current_cn_present = False
        if cn_simp_current and cn_simp_current in original_chinese: is_rule_current_cn_present = True
        if not is_rule_current_cn_present and cn_trad_current and cn_trad_current != cn_simp_current and cn_trad_current in original_chinese: is_rule_current_cn_present = True
        if not is_rule_current_cn_present: continue

        current_rule_made_a_change_in_its_passes = True
        while current_rule_made_a_change_in_its_passes:
            current_rule_made_a_change_in_its_passes = False
            i = 0
            current_scan_len_limit = len(chars)
            while i <= current_scan_len_limit - len_list_bad_en_current:
                if i + len_list_bad_en_current > len(chars): break
                current_segment_chars = chars[i : i + len_list_bad_en_current]
                if current_segment_chars == list_bad_en_current:
                    is_s_locked = any(persistent_active_locks[j] for j in range(i, i + len_list_bad_en_current))
                    if not is_s_locked:
                        is_conflicting_good_translation = False
                        if bad_en_str_current in active_good_translation_owners_map:
                            owner_rule_ids = active_good_translation_owners_map[bad_en_str_current]
                            if any(owner_id != rule_current_id for owner_id in owner_rule_ids):
                                if bad_en_str_current != good_en_str_current:
                                    is_conflicting_good_translation = True
                        if not is_conflicting_good_translation:
                            text_before_apply = "".join(chars)
                            chars[i : i + len_list_bad_en_current] = list_good_en_current
                            current_scan_len_limit = len(chars)
                            text_after_apply = "".join(chars)

                            if text_before_apply != text_after_apply:
                                current_rule_made_a_change_in_its_passes = True
                                rule_applications.append({
                                    'rule_id': rule_current_id, 'bad_translation': bad_en_str_current,
                                    'good_translation': good_en_str_current, 'position': i,
                                    'text_before': text_before_apply, 'text_after': text_after_apply,
                                    'namespace': debug_info[0], 'key': debug_info[1],
                                    'original_chinese': original_chinese
                                })
                            
                            len_diff = len_list_good_en_current - len_list_bad_en_current
                            if len_diff > 0:
                                persistent_active_locks[i + len_list_bad_en_current:i + len_list_bad_en_current] = [False] * len_diff
                            elif len_diff < 0:
                                del persistent_active_locks[i + len_list_good_en_current : i + len_list_bad_en_current]
                            
                            persistent_active_locks[i : i + len_list_good_en_current] = [True] * len_list_good_en_current
                            i += len_list_good_en_current
                        else: i += 1 # Conflicting good translation
                    else: i += 1 # Segment locked
                else: i += 1 # No match
    return "".join(chars), rule_applications

# MODIFIED: Wrapper now unpacks the new 4-element tuple
def post_processing_task_wrapper_mp(args_tuple):
    return execute_single_string_post_processing(*args_tuple)

# --- COMPLETELY REVISED FUNCTION with Aho-Corasick pre-filtering ---
def apply_post_processing(current_translated_data, original_source_data, rules_list_main):
    if not rules_list_main:
        print("No rules loaded for post-processing. Skipping this step.")
        return current_translated_data, {}

    # --- 1. BUILD AUTOMATON (One-time setup) ---
    print("Building Aho-Corasick automaton for rule pre-filtering...")
    A = ahocorasick.Automaton()
    # Use a dictionary to map keyword back to its original rule index(es)
    keyword_to_rule_indices = defaultdict(list)
    for idx, rule in enumerate(rules_list_main):
        simp_cn = rule.get('Simp Chinese')
        if simp_cn and isinstance(simp_cn, str):
            keyword_to_rule_indices[simp_cn].append(idx)
        
        trad_cn = rule.get('Trad Chinese')
        if trad_cn and isinstance(trad_cn, str) and trad_cn != simp_cn:
            keyword_to_rule_indices[trad_cn].append(idx)
            
    for keyword, indices in keyword_to_rule_indices.items():
        A.add_word(keyword, (keyword, indices))
    A.make_automaton()
    print("Automaton build complete.")

    post_processed_data = json.loads(json.dumps(current_translated_data)) # Deep copy
    items_to_process_for_pool = []
    all_rule_applications = defaultdict(list)

    print("Pre-filtering rules for each string...")
    # --- 2. PREPARE WORK ITEMS WITH FILTERED RULES ---
    for namespace, key_value_pairs in tqdm(current_translated_data.items(), desc="Filtering rules for strings"):
        if isinstance(key_value_pairs, dict):
            for key_id, english_text in key_value_pairs.items():
                original_chinese_text = original_source_data.get(namespace, {}).get(key_id)
                if isinstance(english_text, str) and isinstance(original_chinese_text, str):
                    # Find all rule indices that match this Chinese text
                    matching_rule_indices = set()
                    for end_index, (keyword, indices) in A.iter(original_chinese_text):
                        for rule_idx in indices:
                            matching_rule_indices.add(rule_idx)

                    # Create the small, ordered list of applicable rules for the worker
                    if matching_rule_indices:
                        # Sort indices to preserve original rule priority
                        sorted_indices = sorted(list(matching_rule_indices))
                        applicable_rules = [rules_list_main[i] for i in sorted_indices]
                        
                        items_to_process_for_pool.append(
                            (original_chinese_text, english_text, (namespace, key_id), applicable_rules)
                        )
    
    if not items_to_process_for_pool:
        print("No applicable rules found for any strings during post-processing.")
        return post_processed_data, {}

    num_processes = max(1, multiprocessing.cpu_count())
    processed_results_list = []
    print(f"Starting post-processing pool with {num_processes} processes for {len(items_to_process_for_pool)} items...")
    
    # MODIFIED: Removed initializer and initargs from Pool creation
    with multiprocessing.Pool(processes=num_processes) as pool:
        processed_results_list = list(tqdm(pool.imap(post_processing_task_wrapper_mp, items_to_process_for_pool),
                                             total=len(items_to_process_for_pool), desc="Post-processing strings", smoothing=0.05))

    print("Aggregating results...")
    # --- 3. AGGREGATE RESULTS (Logic is the same) ---
    for i, (processed_english_text, rule_applications_for_item) in enumerate(processed_results_list):
        _orig_cn, _orig_en, (namespace, key_id), _rules = items_to_process_for_pool[i]
        if namespace in post_processed_data and isinstance(post_processed_data[namespace], dict) and key_id in post_processed_data[namespace]:
            post_processed_data[namespace][key_id] = processed_english_text
            for app_detail in rule_applications_for_item:
                all_rule_applications[app_detail['rule_id']].append(app_detail)
        else:
            print(f"Warning: Could not map result back for {namespace} -> {key_id} during post-processing update.")

    tracking_report = prepare_tracking_report(all_rule_applications, rules_list_main)
    return post_processed_data, tracking_report

def prepare_tracking_report(all_rule_applications, rules_list):
    # This function remains unchanged
    rule_map = {rule['__rule_id__']: rule for rule in rules_list}
    report = []
    sorted_rule_ids = sorted(all_rule_applications.keys(),
                                 key=lambda rid: len(all_rule_applications[rid]),
                                 reverse=True)
    for rule_id in sorted_rule_ids:
        applications = all_rule_applications[rule_id]
        rule_data = rule_map[rule_id]
        rule_report_entry = {
            'rule_id': rule_id,
            'simp_chinese': rule_data.get('Simp Chinese', ''),
            'trad_chinese': rule_data.get('Trad Chinese', ''),
            'bad_translation': rule_data.get('Bad Translation', ''),
            'good_translation': rule_data.get('Good Translation', ''),
            'application_count': len(applications),
            'applications_details': []
        }
        change_groups = defaultdict(list)
        for app in applications:
            change_key = (app.get('bad_translation', 'N/A'), app.get('good_translation', 'N/A'))
            app_instance_details = {k: app.get(k, 'N/A') for k in ['namespace', 'key', 'position', 'text_before', 'text_after', 'original_chinese']}
            change_groups[change_key].append(app_instance_details)

        for (bad, good), instances in change_groups.items():
            rule_report_entry['applications_details'].append({
                'change_made': f"{bad} → {good}",
                'specific_instance_count': len(instances),
                'instances_preview': instances[:10]
            })
        report.append(rule_report_entry)
    return report

def save_tracking_report(tracking_report, output_path):
    # This function remains unchanged
    if not tracking_report:
        print("No rule applications to track or report is empty.")
        return
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(tracking_report, f, indent=2, ensure_ascii=False)
        total_applications = sum(rule.get('application_count', 0) for rule in tracking_report)
        print(f"\n--- Post-Processing Tracking Summary ---")
        print(f"Total distinct rules applied: {len(tracking_report)}")
        print(f"Total rule application instances: {total_applications}")
        if tracking_report:
            print(f"Top 5 most frequently applied rules:")
            for i, rule in enumerate(tracking_report[:5]):
                print(f"  {i+1}. Rule ID {rule.get('rule_id', 'N/A')}: {rule.get('application_count', 0)} applications")
                print(f"     Rule definition: '{rule.get('bad_translation', 'N/A')}' → '{rule.get('good_translation', 'N/A')}'")
        print(f"\nTracking report saved to: {output_path}")
    except Exception as e:
        print(f"Error saving tracking report: {e}")

# --- Main Execution ---
def main():
    print("Starting script...")
    try:
        with open(UNIFIED_JSON_INPUT_PATH, 'r', encoding='utf-8-sig') as f: unified_data = json.load(f)
        unified_data = normalize_dictionary_keys_recursively(unified_data)
        print(f"Loaded and normalized unified data keys from: {UNIFIED_JSON_INPUT_PATH}")
    except Exception as e: print(f"ERROR loading unified JSON: {e}"); return

    original_source_data_for_postprocessing = json.loads(json.dumps(unified_data))

    translation_map = {}
    try:
        if os.path.exists(TRANSLATION_MAP_PATH):
            with open(TRANSLATION_MAP_PATH, 'r', encoding='utf-8-sig') as f: translation_map = json.load(f)
        else: print(f"INFO: Translation map file not found at '{TRANSLATION_MAP_PATH}'.")
    except Exception as e: print(f"ERROR loading translation map: {e}. Starting with an empty map."); translation_map = {}

    translated_content, untranslated_excerpt = translate_data_with_conditions(unified_data, translation_map)
    print("Initial translation phase complete.")

    excel_rules = load_excel_rules(NORMALISED_MAP_EXCEL_PATH, NORMALISED_MAP_SHEET_NAME)
    final_content_to_save = translated_content
    tracking_report_data = {}

    if excel_rules:
        print(f"Starting post-processing with {len(excel_rules)} rules...")
        final_content_to_save, tracking_report_data = apply_post_processing(translated_content, original_source_data_for_postprocessing, excel_rules)
        print("Post-processing phase complete.")
    else:
        print("Skipping post-processing as no rules were loaded or an error occurred.")

    # --- Saving all output files ---
    def save_json(data, path, name):
        try:
            sorted_data = {}
            for k in sorted(data.keys()):
                sorted_data[k] = dict(sorted(data[k].items())) if isinstance(data[k], dict) else data[k]
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(sorted_data, f, indent=4, ensure_ascii=False)
            print(f"{name} successfully written to: {path}")
        except Exception as e:
            print(f"Error writing {name} to {path}: {e}")

    save_json(final_content_to_save, TRANSLATED_JSON_OUTPUT_PATH, "Final translated data")
    if untranslated_excerpt:
        save_json(untranslated_excerpt, UNTRANSLATED_EXCERPT_PATH, "Untranslated excerpt")
    else:
        print("No untranslated Chinese strings to write to excerpt.")

    if translation_map:
        save_json(translation_map, TRANSLATION_MAP_PATH, "Updated translation map")
    else:
        print("Translation map is empty or was not modified, not saving.")
    
    if tracking_report_data:
        save_tracking_report(tracking_report_data, RULE_TRACKING_OUTPUT_PATH)

    print("Script finished.")

if __name__ == "__main__":
    multiprocessing.freeze_support() # For PyInstaller/multiprocessing on Windows
    main()