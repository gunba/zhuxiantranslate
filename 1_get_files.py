import subprocess
import os
import shutil # Import the shutil module for rmtree

# --- Configuration ---
CLI_EXE_PATH = "C:/Users/jorda/PycharmProjects/zxsjlocpipe/files_cli/bin/Debug/net9.0/files_cli.exe"
AES_KEY = "0xD7D19A4349AAA02C53CD9282D0E3B5B8BEE592829DD2DF729EBD0D377E4CC47D"
BASE_OUTPUT_DIR = "C:/Users/jorda/PycharmProjects/zxsjlocpipe/zxsj_output"

GAME_VERSIONS_CONFIG = {
    # CN Version - Main Files and additional localizations
    "ZXSJ_Speed_S1": {
        "paks_dir": r"C:\Users\jorda\PycharmProjects\zxsjlocpipe\china_s1_paks",
        "pak_groups": {
            "pakchunk0": { # For the primary zh-Hans Game.locres
                "base": "pakchunk0-Windows.pak",
                "patch": "pakchunk0-Windows_0_P.pak",
                "targets": [
                    {"type": "GetFile", "path": "ZhuxianClient/Content/Localization/Game/zh-Hans/Game.locres"}
                ]
            },
            "ClientGameData": { # For FormatString data
                "base": "ClientGameData.pak",
                "patch": "ClientGameData_0_P.pak",
                "targets": [
                     {"type": "GetFolder", "path": "ZhuxianClient/gamedata/client/FormatString"}
                ]
            }
        }
    },
    "ZXSJ_Speed": { # This represents "CN"
        "paks_dir": r"C:\Program Files\ZXSJclient\ZXSJ_Speed\Game\ZhuxianClient\Content\Paks",
        "pak_groups": {
            "pakchunk0": { # For the primary zh-Hans Game.locres
                "base": "pakchunk0-Windows.pak",
                "patch": "pakchunk0-Windows_0_P.pak",
                "targets": [
                    {"type": "GetFile", "path": "ZhuxianClient/Content/Localization/Game/zh-Hans/Game.locres"}
                ]
            },
            "ClientGameData": { # For FormatString data
                "base": "ClientGameData.pak",
                "patch": "ClientGameData_0_P.pak",
                "targets": [
                     {"type": "GetFolder", "path": "ZhuxianClient/gamedata/client/FormatString"}
                ]
            },
            "pakchunk16_UI_JSON": { # For FormatString data
                "base": "pakchunk16-Windows.pak",
                "patch": "pakchunk16-Windows_0_P.pak",
                "targets": [
                     {"type": "GetFolderAsJson", "path": "ZhuxianClient/Content/UI"}
                ]
            }
        }
    },
    "ZXSJ_Speed_EN": { # For CN English LocRes from ZCTranslateData
        "paks_dir": r"C:\Program Files\ZXSJclient\ZXSJ_Speed\Game\ZhuxianClient\Content\Paks",
        "pak_groups": {
            "pakchunk0": {
                "base": "ClientGameData.pak",
                "patch": "ClientGameData_0_P.pak",
                "targets": [
                    {"type": "GetFile", "path": "ZhuxianClient/gamedata/client/ZCTranslateData/Game/en/Game.locres"}
                ]
            }
        }
    },
    "ZXSJ_Speed_RU": { # For CN Russian LocRes from ZCTranslateData
        "paks_dir": r"C:\Program Files\ZXSJclient\ZXSJ_Speed\Game\ZhuxianClient\Content\Paks",
        "pak_groups": {
            "pakchunk0": {
                "base": "ClientGameData.pak",
                "patch": "ClientGameData_0_P.pak",
                "targets": [
                    {"type": "GetFile", "path": "ZhuxianClient/gamedata/client/ZCTranslateData/Game/ru/Game.locres"}
                ]
            }
        }
    },
    "ZXSJ_Speed_TW": { # For CN zh-Hant LocRes from ZCTranslateData
        "paks_dir": r"C:\Program Files\ZXSJclient\ZXSJ_Speed\Game\ZhuxianClient\Content\Paks",
        "pak_groups": {
            "pakchunk0": {
                "base": "ClientGameData.pak",
                "patch": "ClientGameData_0_P.pak",
                "targets": [
                    {"type": "GetFile", "path": "ZhuxianClient/gamedata/client/ZCTranslateData/Game/zh-Hant/Game.locres"}
                ]
            }
        }
    },
    "ZXSJ_Speed_CN": { # For CN zh-Hans LocRes from ZCTranslateData
        "paks_dir": r"C:\Program Files\ZXSJclient\ZXSJ_Speed\Game\ZhuxianClient\Content\Paks",
        "pak_groups": {
            "pakchunk0": {
                "base": "ClientGameData.pak",
                "patch": "ClientGameData_0_P.pak",
                "targets": [
                    {"type": "GetFile", "path": "ZhuxianClient/gamedata/client/ZCTranslateData/Game/zh-Hans/Game.locres"}
                ]
            }
        }
    },

    # TW Version - Main Files and additional localizations
    "zxsjgt": { # This represents "TW (other)"
        "paks_dir": r"C:\Program Files\zxsjgt\ZXSJ_Speed\Game\ZhuxianClient\Content\Paks",
        "pak_groups": {
            "pakchunk0": { # For the primary zh-Hant Game.locres
                "base": "pakchunk0-Windows.pak",
                "patch": "pakchunk0-Windows_0_P.pak",
                "targets": [
                    {"type": "GetFile", "path": "ZhuxianClient/Content/Localization/Game/zh-Hant/Game.locres"}
                ]
            },
            "ClientGameData": { # For FormatString data
                "base": "ClientGameData.pak",
                "patch": "ClientGameData_0_P.pak",
                "targets": [
                     {"type": "GetFolder", "path": "ZhuxianClient/gamedata/client/FormatString"}
                ]
            },
            "pakchunk16_UI_JSON": { # For FormatString data
                "base": "pakchunk16-Windows.pak",
                "patch": "pakchunk16-Windows_0_P.pak",
                "targets": [
                     {"type": "GetFolderAsJson", "path": "ZhuxianClient/Content/UI"}
                ]
            }
        }
    },
    "zxsjgt_EN": { # For TW English LocRes from ZCTranslateData
        "paks_dir": r"C:\Program Files\zxsjgt\ZXSJ_Speed\Game\ZhuxianClient\Content\Paks",
        "pak_groups": {
            "pakchunk0": {
                "base": "ClientGameData.pak",
                "patch": "ClientGameData_0_P.pak",
                "targets": [
                    {"type": "GetFile", "path": "ZhuxianClient/gamedata/client/ZCTranslateData/Game/en/Game.locres"}
                ]
            }
        }
    },
    "zxsjgt_RU": { # For TW Russian LocRes from ZCTranslateData
        "paks_dir": r"C:\Program Files\zxsjgt\ZXSJ_Speed\Game\ZhuxianClient\Content\Paks",
        "pak_groups": {
            "pakchunk0": {
                "base": "ClientGameData.pak",
                "patch": "ClientGameData_0_P.pak",
                "targets": [
                    {"type": "GetFile", "path": "ZhuxianClient/gamedata/client/ZCTranslateData/Game/ru/Game.locres"}
                ]
            }
        }
    },
    "zxsjgt_TW": { # For TW zh-Hant LocRes from ZCTranslateData
        "paks_dir": r"C:\Program Files\zxsjgt\ZXSJ_Speed\Game\ZhuxianClient\Content\Paks",
        "pak_groups": {
            "pakchunk0": {
                "base": "ClientGameData.pak",
                "patch": "ClientGameData_0_P.pak",
                "targets": [
                    {"type": "GetFile", "path": "ZhuxianClient/gamedata/client/ZCTranslateData/Game/zh-Hant/Game.locres"}
                ]
            }
        }
    },
    "zxsjgt_CN": { # For TW zh-Hans LocRes from ZCTranslateData
        "paks_dir": r"C:\Program Files\zxsjgt\ZXSJ_Speed\Game\ZhuxianClient\Content\Paks",
        "pak_groups": {
            "pakchunk0": {
                "base": "ClientGameData.pak",
                "patch": "ClientGameData_0_P.pak",
                "targets": [
                    {"type": "GetFile", "path": "ZhuxianClient/gamedata/client/ZCTranslateData/Game/zh-Hans/Game.locres"}
                ]
            }
        }
    }
}

# --- Helper Functions ---
# (run_pak_extractor_command and extract_targets_for_pak_group remain unchanged)
def run_pak_extractor_command(paks_root_directory, output_path_for_target, command, export_target_path=None):
    """
    Runs the PakExtractorCli.exe with the given arguments.
    paks_root_directory: The root folder containing all .pak files for the C# CLI to scan.
    output_path_for_target: The directory where the C# CLI should place the extracted file/folder.
    """
    if not os.path.exists(CLI_EXE_PATH):
        print(f"FATAL ERROR: PakExtractorCli.exe not found at {CLI_EXE_PATH}")
        print("Please update the CLI_EXE_PATH variable in the script.")
        return False

    cmd_args = [
        CLI_EXE_PATH,
        paks_root_directory,
        AES_KEY,
        output_path_for_target,
        command
    ]
    if export_target_path:
        cmd_args.append(export_target_path)

    try:
        print(f"\nRunning command: {' '.join(cmd_args)}")
        process = subprocess.Popen(cmd_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='replace')
        stdout, stderr = process.communicate()

        if stdout:
            print("Output:")
            # Filter out most [Info] lines unless they indicate success/error/warning
            filtered_stdout = [line for line in stdout.splitlines() if not line.startswith("[Info]") or \
                               "exported" in line.lower() or \
                               "extracted" in line.lower() or \
                               "Error" in line.lower() or \
                               "SUCCESS" in line or \
                               "WARNING" in line.lower()] # made WARNING case-insensitive
            for line in filtered_stdout:
                print(line)
        if stderr: # Print any errors from CLI
            print("CLI Errors:")
            print(stderr)
        if process.returncode != 0:
            print(f"PakExtractorCli returned a non-zero exit code: {process.returncode}")
            return False
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error executing PakExtractorCli: {e}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        print(f"Error: PakExtractorCli.exe not found at {CLI_EXE_PATH}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred while running CLI: {e}")
        return False

def extract_targets_for_pak_group(game_version_name, game_version_paks_root_dir, targets_list, group_specific_output_dir_root):
    """
    Extracts specified files or folders for a game version.
    CUE4Parse in the CLI is initialized with game_version_paks_root_dir and handles pak priority.
    """
    print(f"  Requesting targets for {game_version_name} (pak group output: {os.path.basename(group_specific_output_dir_root)}), base output for this group: {group_specific_output_dir_root}")

    success = True
    for target_info in targets_list:
        command_type = target_info["type"]
        target_path_in_pak = target_info["path"]
        # For GetFile, the cli_output_path is the directory where the file will be placed.
        # For GetFolder, the cli_output_path is the parent directory under which the target folder structure will be replicated.
        cli_output_path = group_specific_output_dir_root

        if not run_pak_extractor_command(
            game_version_paks_root_dir,
            cli_output_path,
            command_type,
            target_path_in_pak
        ):
            print(f"    Failed to process {target_path_in_pak} (command: {command_type}) for {game_version_name}")
            success = False
    return success

# --- Main Script Logic ---
def main():
    # --- Delete BASE_OUTPUT_DIR if it exists ---
    if os.path.exists(BASE_OUTPUT_DIR):
        print(f"Clearing existing output directory: {BASE_OUTPUT_DIR}")
        try:
            shutil.rmtree(BASE_OUTPUT_DIR)
            print(f"Successfully removed: {BASE_OUTPUT_DIR}")
        except OSError as e:
            print(f"Error removing directory {BASE_OUTPUT_DIR}: {e.strerror}. Please check permissions or close any programs using files in this directory.")
            return # Exit if we can't clear the directory
    # --- End Deletion ---

    print(f"Starting extraction process. Output will be in: {BASE_OUTPUT_DIR}")
    try:
        os.makedirs(BASE_OUTPUT_DIR)
        print(f"Successfully created base output directory: {BASE_OUTPUT_DIR}")
    except OSError as e:
        print(f"Error creating base output directory {BASE_OUTPUT_DIR}: {e.strerror}.")
        return


    for version_name, config in GAME_VERSIONS_CONFIG.items():
        print(f"\n--- Processing Game Version: {version_name} ---")
        game_version_paks_root_dir = config["paks_dir"]

        if not os.path.isdir(game_version_paks_root_dir):
            print(f"  WARNING: Paks root directory not found for {version_name}: {game_version_paks_root_dir}. Skipping.")
            continue

        for pak_group_name, pak_info in config["pak_groups"].items():
            print(f"  -- Processing Pak Group: {pak_group_name} --")

            # Output path for this specific game version and pak group
            specific_pak_group_output_dir = os.path.join(BASE_OUTPUT_DIR, version_name, pak_group_name)
            os.makedirs(specific_pak_group_output_dir, exist_ok=True)


            base_pak_full_path = os.path.join(game_version_paks_root_dir, pak_info["base"])
            if not os.path.exists(base_pak_full_path):
                print(f"    INFO: Base pak {pak_info['base']} not found in {game_version_paks_root_dir}. Will rely on other paks if available for this group.")

            if pak_info.get("patch"):
                patch_pak_full_path = os.path.join(game_version_paks_root_dir, pak_info["patch"])
                if not os.path.exists(patch_pak_full_path):
                    print(f"    INFO: Patch pak {pak_info['patch']} not found in {game_version_paks_root_dir}. This might be normal for this group.")

            extract_targets_for_pak_group(
                version_name,
                game_version_paks_root_dir, # Pass the root directory of all paks for this game version
                pak_info["targets"],
                specific_pak_group_output_dir # Pass the specific output dir for this pak_group's extractions
            )

            print(f"  Finished processing Pak Group: {pak_group_name} for {version_name}. Output intended for: {specific_pak_group_output_dir}")

    print("\n--- Extraction process completed. ---")

if __name__ == "__main__":
    if not os.path.exists(CLI_EXE_PATH) or not os.path.isfile(CLI_EXE_PATH):
        print(f"FATAL ERROR: PakExtractorCli.exe not found or is not a file at '{CLI_EXE_PATH}'")
        print("Please update the CLI_EXE_PATH variable at the top of the Python script.")
    else:
        main()