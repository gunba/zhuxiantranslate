import os
import subprocess
import shutil
import sys
import time
import ctypes # For admin check and elevation

def is_admin():
    """Checks if the current script is running with admin privileges on Windows."""
    if os.name == 'nt':
        try:
            return ctypes.windll.shell32.IsUserAnAdmin() != 0
        except AttributeError:
            # This can happen if shell32 is not found or IsUserAnAdmin is not available
            print("WARNING: Could not determine admin status via ctypes.")
            return False
    return True # Non-Windows systems are assumed to not need this specific check

def request_admin_privileges(main_function_to_call):
    """
    If not running as admin on Windows, attempts to re-launch the script
    with admin privileges. Calls the provided main_function_to_call if privileges are met.
    """
    if os.name != 'nt':
        print("INFO: Not a Windows system, proceeding without explicit admin elevation.")
        main_function_to_call()
        return

    if is_admin():
        print("INFO: Running with administrator privileges.")
        main_function_to_call()
        return
    else:
        print("INFO: Administrator privileges are required for some operations.")
        print("INFO: Attempting to re-launch with administrator privileges...")

        try:
            script_path_abs = os.path.abspath(__file__)
            # Construct parameters for python.exe: "script_path_quoted" "arg1_quoted" "arg2_quoted" ...
            params_list = ['"%s"' % script_path_abs]  # Script path itself
            params_list.extend(['"%s"' % arg for arg in sys.argv[1:]]) # Then other original arguments
            parameters_for_python_exe = ' '.join(params_list)

            ret_code = ctypes.windll.shell32.ShellExecuteW(
                None,                           # hwnd
                "runas",                        # lpOperation (requests elevation)
                sys.executable,                 # lpFile (path to python.exe)
                parameters_for_python_exe,      # lpParameters (script path + its args)
                os.getcwd(),                    # lpDirectory (start in the current working directory)
                1                               # nShowCmd (SW_SHOWNORMAL)
            )

            if ret_code > 32:
                # ShellExecuteW call was successful, UAC prompt shown (or auto-elevated).
                # The new elevated process will start. The current non-admin script should exit.
                print("INFO: UAC prompt initiated or auto-elevation occurred. The current non-admin script instance will now exit.")
                sys.exit(0)
            else:
                # ShellExecuteW call failed before UAC or UAC was denied.
                if ret_code == 1223: # ERROR_CANCELLED
                    print("INFO: UAC prompt was cancelled by the user. Script cannot proceed without admin rights.")
                else:
                    print(f"ERROR: Failed to elevate privileges (ShellExecuteW error code: {ret_code}).")
                print("       Please try running the script as an administrator manually.")
                sys.exit(1) # Exit because elevation failed or was cancelled

        except Exception as e:
            print(f"ERROR: An exception occurred while trying to re-launch as administrator: {e}")
            sys.exit(1)

# --- Configuration ---
# Updated to include both target directories
TARGET_PAK_DIRS = [
    r"C:\Program Files\zxsjgt\ZXSJ_Speed\Game\ZhuxianClient\Content\Paks",
    r"C:\Program Files\ZXSJclient\ZXSJ_Speed\Game\ZhuxianClient\Content\Paks"
]
PATCH_FILENAME = "~Eng_Patch_P.pak" # This is the expected output .pak file

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

PYTHON_SCRIPT_NAMES = [
    "1_get_files.py",
    "2_normalize_files.py",
    "3_translate_unified_json.py",
    "4_generate_translations.py", # This script should create the FOLDER_TO_PACK_NAME
]
PYTHON_SCRIPTS_FULL_PATHS = [os.path.join(SCRIPT_DIR, name) for name in PYTHON_SCRIPT_NAMES]

# Path where the new .pak file is expected to be created by repak (in SCRIPT_DIR)
NEW_PATCH_SOURCE_PATH = os.path.join(SCRIPT_DIR, PATCH_FILENAME)

# Define the source folder for repak (relative to SCRIPT_DIR)
FOLDER_TO_PACK_NAME = "~Eng_Patch_P"
FOLDER_TO_PACK_FULL_PATH = os.path.join(SCRIPT_DIR, FOLDER_TO_PACK_NAME)
# --- End Configuration ---

def run_command(command_args, description, use_shell=False, cwd=None):
    """
    Runs a shell command and prints its output.
    Args:
        command_args (list or str): The command and its arguments.
        description (str): A description of what the command is doing.
        use_shell (bool): Whether to use shell=True with subprocess.run.
        cwd (str, optional): The working directory for the command. Defaults to None.
    Returns:
        bool: True if the command was successful, False otherwise.
    """
    print(f"\nINFO: Starting: {description}")
    if cwd:
        print(f"INFO: Setting working directory for this step to: {cwd}")
    try:
        process = subprocess.run(
            command_args,
            check=True,
            capture_output=True,
            text=True,
            shell=use_shell,
            cwd=cwd,
            # encoding='utf-8', errors='replace' # Uncomment if output issues
        )
        print(f"INFO: Successfully completed: {description}")
        if process.stdout:
            print("--- STDOUT ---")
            print(process.stdout.strip())
            print("--- END STDOUT ---")
        if process.stderr: # Some tools might output to stderr even on success
            print("--- STDERR ---")
            print(process.stderr.strip())
            print("--- END STDERR ---")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed step: {description}")
        print(f"  Return code: {e.returncode}")
        if e.stdout:
            print("--- STDOUT (on error) ---")
            print(e.stdout.strip())
            print("--- END STDOUT (on error) ---")
        if e.stderr:
            print("--- STDERR (on error) ---")
            print(e.stderr.strip())
            print("--- END STDERR (on error) ---")
        return False
    except FileNotFoundError:
        cmd_display = command_args[0] if isinstance(command_args, list) else command_args
        print(f"ERROR: Command not found for step: {description}.")
        print(f"  Ensure '{cmd_display}' is a valid command, in PATH, or the path is correct.")
        print(f"  Attempted to run: {command_args}")
        if cwd:
            print(f"  In working directory: {cwd}")
        return False
    except Exception as e:
        print(f"ERROR: An unexpected error occurred during: {description}")
        print(f"  Error details: {str(e)}")
        return False

def main():
    print("INFO: Starting the automated patch process...")
    print("IMPORTANT: This script may require administrator privileges for file operations in Program Files.")
    print(f"INFO: Script directory (base for operations and intermediate files): {SCRIPT_DIR}")
    print(f"INFO: Original current working directory (when this script started): {os.getcwd()}")
    print(f"INFO: Target Paks directories:")
    for target_dir in TARGET_PAK_DIRS:
        print(f"  - {target_dir}")

    # 1. Delete existing patch file(s)
    print(f"\n--- Step 1: Delete existing patch file(s) ---")
    for target_dir in TARGET_PAK_DIRS:
        old_patch_full_path = os.path.join(target_dir, PATCH_FILENAME)
        print(f"INFO: Checking for existing patch in: {target_dir}")
        if os.path.exists(old_patch_full_path):
            print(f"INFO: Found existing patch: {old_patch_full_path}")
            try:
                os.remove(old_patch_full_path)
                print(f"INFO: Successfully deleted old patch file from {target_dir}.")
            except PermissionError:
                print(f"ERROR: Permission denied. Could not delete {old_patch_full_path}.")
                print(f"       Please ensure this script is running as an administrator.")
                return # Exit if any deletion fails due to permissions
            except Exception as e:
                print(f"ERROR: Failed to delete {old_patch_full_path}. Reason: {e}")
                return # Exit if any deletion fails for other reasons
        else:
            print(f"INFO: Old patch file {PATCH_FILENAME} not found in {target_dir}. No deletion needed there.")

    # 2-5. Run Python processing scripts
    print(f"\n--- Steps 2-5: Running Python processing scripts ---")
    for i, script_full_path in enumerate(PYTHON_SCRIPTS_FULL_PATHS):
        script_name = os.path.basename(script_full_path)
        step_number = 2 + i
        description = f"Step {step_number}: Running Python script '{script_name}'"
        # Ensure these scripts run from SCRIPT_DIR if they expect relative paths
        if not run_command([sys.executable, script_full_path], description, cwd=SCRIPT_DIR):
            print(f"ERROR: Process halted due to failure in {script_name}.")
            return

    # --- Step 6: Create English Patch using repak ---
    print(f"\n--- Step 6: Creating patch using 'repak' tool ---")
    print(f"INFO: Source folder to pack: {FOLDER_TO_PACK_FULL_PATH}")
    print(f"INFO: Expected output patch file in script directory: {NEW_PATCH_SOURCE_PATH}")

    # 6a. Check if the source folder (to be packed by repak) exists
    if not os.path.isdir(FOLDER_TO_PACK_FULL_PATH):
        print(f"ERROR: Source folder '{FOLDER_TO_PACK_FULL_PATH}' not found.")
        print(f"       This folder should have been created by a previous script (e.g., 4_generate_translations.py).")
        return

    # 6b. Construct and run the repak command
    repak_executable = "repak" # Assumes 'repak' is in PATH or SCRIPT_DIR (if PATH includes SCRIPT_DIR)
    # To specify repak in SCRIPT_DIR explicitly:
    # repak_exe_path_check = os.path.join(SCRIPT_DIR, "repak.exe")
    # if os.path.exists(repak_exe_path_check):
    #    repak_executable = repak_exe_path_check
    # else: # fallback or error
    #    repak_executable = "repak" # or print error and exit if it must be local

    # 'repak pack' command takes the folder name (relative to cwd)
    # Adding --compression Oodle as it was in the original context (though not visible in provided snippet)
    # If repak needs full path for folder, use FOLDER_TO_PACK_FULL_PATH.
    # Assuming FOLDER_TO_PACK_NAME is correct when cwd=SCRIPT_DIR.
    repak_command_args = [repak_executable, "pack", FOLDER_TO_PACK_NAME] # Add "--compression", "Oodle" if needed
    description = f"Step 6b: Running repak: {' '.join(repak_command_args)}"

    if not run_command(repak_command_args, description, cwd=SCRIPT_DIR):
        print(f"ERROR: 'repak' command failed. Process halted.")
        return

    # 6c. Verify that repak created the output file
    print(f"INFO: repak command completed. Verifying output file presence...")
    time.sleep(1) # Wait 1 second for filesystem to catch up

    if os.path.exists(NEW_PATCH_SOURCE_PATH):
        print(f"INFO: Successfully verified patch file creation: {NEW_PATCH_SOURCE_PATH}")
    else:
        print(f"ERROR: 'repak' command appeared to succeed, but the expected patch file")
        print(f"       '{NEW_PATCH_SOURCE_PATH}' was NOT found in {SCRIPT_DIR}.")
        print(f"       Please check the 'repak' tool's output and behavior.")
        return

    # --- Step 7: Copying new patch file to target directories ---
    print(f"\n--- Step 7: Copying new patch file to target directories ---")
    if not os.path.exists(NEW_PATCH_SOURCE_PATH):
        # This check is somewhat redundant due to 6c, but good for safety.
        print(f"ERROR: New patch file '{NEW_PATCH_SOURCE_PATH}' was NOT found in the script directory ({SCRIPT_DIR}) for copying.")
        return

    for target_dir in TARGET_PAK_DIRS:
        print(f"INFO: Attempting to copy new patch to: {target_dir}")
        try:
            # Ensure target directory exists before copying
            if not os.path.isdir(target_dir):
                print(f"WARNING: Target directory '{target_dir}' does not exist. Skipping copy to this location.")
                # Depending on requirements, you might want to create it:
                # try:
                #     os.makedirs(target_dir, exist_ok=True)
                #     print(f"INFO: Created missing target directory: {target_dir}")
                # except Exception as e_mkdir:
                #     print(f"ERROR: Could not create missing target directory {target_dir}. Reason: {e_mkdir}")
                #     continue # Skip to next target_dir
                continue # Skip if directory doesn't exist and we are not creating it

            destination_path = os.path.join(target_dir, PATCH_FILENAME)
            shutil.copy(NEW_PATCH_SOURCE_PATH, destination_path)
            print(f"INFO: Successfully copied '{PATCH_FILENAME}' to '{destination_path}'")
        except PermissionError:
            print(f"ERROR: Permission denied. Could not copy '{PATCH_FILENAME}' to '{target_dir}'.")
            print(f"       Please ensure you have administrator privileges.")
            # If one copy fails due to permission, we might want to stop or try others.
            # Current behavior: return, which stops all further processing.
            return
        except FileNotFoundError: # Should not happen if NEW_PATCH_SOURCE_PATH check passed, but for safety
            print(f"ERROR: Source file '{NEW_PATCH_SOURCE_PATH}' suddenly not found during copy to {target_dir}.")
            return
        except shutil.SameFileError: # Should not happen with this logic
            print(f"ERROR: Source and destination are the same file for {target_dir}. This should not happen.")
            return
        except Exception as e:
            print(f"ERROR: Failed to copy '{PATCH_FILENAME}' to '{target_dir}'. Reason: {e}")
            return # Stop if any copy fails

    print(f"\nSUCCESS: All steps completed. The patch '{PATCH_FILENAME}' should now be updated in the configured target directories.")

if __name__ == "__main__":
    # This function will handle asking for admin rights if needed (on Windows)
    # and then call main()
    request_admin_privileges(main)
