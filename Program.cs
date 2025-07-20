using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CUE4Parse.Encryption.Aes;
using CUE4Parse.FileProvider;
using CUE4Parse.FileProvider.Objects; // Make sure this is present
using CUE4Parse.UE4.Objects.Core.Misc;
using CUE4Parse.UE4.Versions;
using Newtonsoft.Json;
using CUE4Parse.UE4.Assets.Exports;
// using CUE4Parse.UE4.Assets.Objects; // Not strictly needed for Program.cs, but good if you expand
using CUE4Parse.UE4.Assets;          // For IPackage
using CUE4Parse.Utils;               // For string extensions
using CUE4Parse.Compression;         // For OodleHelper

namespace PakExtractorCli
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            // --- OODLE INITIALIZATION ---
            // This should be one of the first things your application does.
            // Ensure the Oodle DLL (e.g., oo2core_9_win64.dll) is in the same directory
            // as PakExtractorCli.exe, or provide the correct path to it.
            try
            {
                string oodleDllName = OodleHelper.OODLE_DLL_NAME; // e.g. "oo2core_9_win64.dll"
                string oodlePath = Path.Combine(AppContext.BaseDirectory, oodleDllName);

                if (File.Exists(oodlePath))
                {
                    OodleHelper.Initialize(oodlePath);
                    Console.WriteLine($"[INFO] Oodle initialized with: {oodlePath}");
                }
                else
                {
                    // Attempt to download if not found - might be problematic in some environments
                    // For a CLI, it's often better to require the user to place it manually.
                    Console.WriteLine($"[WARNING] Oodle DLL '{oodleDllName}' not found in application directory ('{AppContext.BaseDirectory}'). Attempting to download...");
                    if (OodleHelper.DownloadOodleDllAsync(oodlePath).GetAwaiter().GetResult())
                    {
                        OodleHelper.Initialize(oodlePath);
                        Console.WriteLine($"[INFO] Oodle downloaded and initialized to: {oodlePath}");
                    }
                    else
                    {
                        Console.WriteLine($"[ERROR] Failed to download Oodle DLL. Oodle-compressed files may fail to extract.");
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"[WARNING] Failed to initialize Oodle: {e.Message}. Extraction of Oodle-compressed files might fail.");
            }
            // --- END OODLE INITIALIZATION ---


            if (args.Length < 4)
            {
                Console.WriteLine("Usage: PakExtractorCli <PaksDirectory> <AesKey> <OutputPath> <ExportCommand> [ExportPath]");
                Console.WriteLine("ExportCommand: GetFile, GetFolder, GetFolderAsJson");
                Console.WriteLine("Example: PakExtractorCli \"C:\\Paks\" \"0xYourAesKey...\" \"C:\\Output\" GetFile \"Game/Path/To/File.uasset\"");
                return;
            }

            string paksDirectory = args[0];
            string aesKey = args[1];
            string outputPath = args[2];
            string exportCommand = args[3];
            string? exportPath = args.Length > 4 ? args[4] : null;

            if (!Directory.Exists(paksDirectory))
            {
                Console.WriteLine($"[ERROR] Paks directory not found: {paksDirectory}");
                return;
            }

            Directory.CreateDirectory(outputPath);

            // Consider if a more specific EGame enum exists for ZhuxianClient for better compatibility
            var provider = new DefaultFileProvider(paksDirectory, SearchOption.AllDirectories, true, new VersionContainer(EGame.GAME_UE4_LATEST));
            provider.Initialize();
            Console.WriteLine($"[INFO] CUE4Parse Provider ProjectName determined as: '{provider.ProjectName}'"); // Log ProjectName
            provider.SubmitKey(new FGuid(), new FAesKey(aesKey));

            try
            {
                switch (exportCommand.ToLower())
                {
                    case "getfile":
                        if (string.IsNullOrEmpty(exportPath))
                        {
                            Console.WriteLine("[ERROR] ExportPath is required for GetFile command.");
                            return;
                        }
                        ExtractSpecificFile(provider, exportPath, outputPath);
                        break;
                    case "getfolder":
                        if (string.IsNullOrEmpty(exportPath))
                        {
                            Console.WriteLine("[ERROR] ExportPath is required for GetFolder command.");
                            return;
                        }
                        ExtractFolderContents(provider, exportPath, outputPath, false);
                        break;
                    case "getfolderasjson":
                        if (string.IsNullOrEmpty(exportPath))
                        {
                            Console.WriteLine("[ERROR] ExportPath is required for GetFolderAsJson command.");
                            return;
                        }
                        ExtractFolderContents(provider, exportPath, outputPath, true);
                        break;
                    default:
                        Console.WriteLine($"[ERROR] Unknown command: {exportCommand}");
                        break;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"[FATAL ERROR] An unhandled exception occurred: {e.Message}");
                Console.WriteLine(e.StackTrace);
            }
        }

        // ... (ExtractSpecificFile method with the detailed debugging from the previous response) ...
        // ... (ExtractFolderContents method with the fixes for folder path handling) ...
        // Make sure these methods are part of your Program class
        // The ExtractSpecificFile method from the previous response is good for debugging the Game.locres issue.
        // The ExtractFolderContents method from two responses ago (that fixed FormatString) should be used.

        // Placeholder for the debug-enhanced ExtractSpecificFile from previous step
        private static void ExtractSpecificFile(AbstractFileProvider provider, string filePath, string outputDirectory)
        {
            string originalFilePath = filePath;
            Console.WriteLine($"[INFO] Attempting to process file: {originalFilePath}");

            if (provider.TryGetGameFile(originalFilePath, out GameFile? gameFileEntry))
            {
                Console.WriteLine($"[DEBUG] Successfully found GameFile entry for: {originalFilePath} (Size: {gameFileEntry.Size}, IsEncrypted: {gameFileEntry.IsEncrypted}, Compression: {gameFileEntry.CompressionMethod})");
                if (gameFileEntry.TryRead(out byte[]? data) && data != null)
                {
                    Console.WriteLine($"[DEBUG] Successfully read data for: {originalFilePath} (Data Length: {data.Length})");
                    string fileName = Path.GetFileName(originalFilePath);
                    string outPath = Path.Combine(outputDirectory, fileName);
                    try
                    {
                        File.WriteAllBytes(outPath, data);
                        Console.WriteLine($"[SUCCESS] Extracted: {outPath}");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"[ERROR] Failed to write file {outPath}: {e.Message}");
                    }
                }
                else
                {
                    Console.WriteLine($"[ERROR] Found GameFile entry for {originalFilePath}, but FAILED TO READ ITS DATA.");
                }
            }
            else
            {
                string fixedPathAttempt = provider.FixPath(originalFilePath);
                Console.WriteLine($"[ERROR] GameFile entry NOT FOUND in provider for: {originalFilePath}");
                Console.WriteLine($"[DEBUG] CUE4Parse FixPath attempt for this input was: '{fixedPathAttempt}'");
                // ... (rest of the debug output from previous response to list candidate keys) ...
                string searchFileName = Path.GetFileName(originalFilePath);
                var similarFiles = provider.Files.Keys
                    .Where(k => k.EndsWith(searchFileName, StringComparison.OrdinalIgnoreCase) &&
                                k.Contains("Localization", StringComparison.OrdinalIgnoreCase))
                    .Take(20)
                    .ToList();
                if (similarFiles.Any())
                {
                    Console.WriteLine("[DEBUG] Potentially similar files found in provider:");
                    similarFiles.ForEach(k => Console.WriteLine($"[DEBUG]   {k}"));
                }
                else
                {
                    Console.WriteLine("[DEBUG] No similar files found with that name in Localization folders.");
                }
            }
        }

        // Use the ExtractFolderContents method that was confirmed to work for "FormatString"
        private static void ExtractFolderContents(AbstractFileProvider provider, string folderPath, string outputDirectory, bool asJson)
        {
            string normalizedUserInputPath = folderPath.Replace('\\', '/');
            string prefixToSearch;

            // Path fixing logic (from previous working version for FormatString)
            if (normalizedUserInputPath.StartsWith("/Game/", StringComparison.OrdinalIgnoreCase))
            {
                prefixToSearch = provider.FixPath(normalizedUserInputPath.TrimEnd('/') + "/dummy.file").SubstringBeforeLast('/');
            }
            else if (provider.ProjectName != null && normalizedUserInputPath.StartsWith(provider.ProjectName + "/", StringComparison.OrdinalIgnoreCase))
            {
                prefixToSearch = provider.FixPath(normalizedUserInputPath.TrimEnd('/') + "/dummy.file").SubstringBeforeLast('/');
            }
            else
            {
                // Try to make it absolute-like for FixPath to handle ProjectName prefixing for paths like "ZhuxianClient/Content/UI"
                string pathForFixer = "/" + (provider.ProjectName ?? "").Trim('/') + "/" + normalizedUserInputPath.Trim('/');
                prefixToSearch = provider.FixPath(pathForFixer.TrimEnd('/') + "/dummy.file").SubstringBeforeLast('/');
            }
            prefixToSearch = prefixToSearch.TrimEnd('/') + "/";

            var filesInFolder = provider.Files
                .Where(kvp => kvp.Key.StartsWith(prefixToSearch, StringComparison.OrdinalIgnoreCase))
                .ToList();

            if (!filesInFolder.Any())
            {
                // Fallback if the above logic didn't yield results
                string directPrefix = normalizedUserInputPath.TrimEnd('/') + "/";
                filesInFolder = provider.Files
                    .Where(kvp => kvp.Key.StartsWith(directPrefix, StringComparison.OrdinalIgnoreCase))
                    .ToList();

                if (filesInFolder.Any())
                {
                    prefixToSearch = directPrefix; // Use the direct prefix if it worked
                }
                else
                {
                    Console.WriteLine($"[ERROR] Folder not found or is empty: {folderPath} (Attempted to match with prefix: {prefixToSearch})");
                    return;
                }
            }

            string folderNameForOutputPath = Path.GetFileName(normalizedUserInputPath.TrimEnd('/')); // e.g., "UI"
            string currentOutputDirectory = Path.Combine(outputDirectory, folderNameForOutputPath);
            Directory.CreateDirectory(currentOutputDirectory);
            Console.WriteLine($"[INFO] Processing folder '{folderPath}' for output to '{currentOutputDirectory}'");

            int processedCount = 0;
            foreach (var gameFilePair in filesInFolder)
            {
                var gameFile = gameFilePair.Value;
                string fileName = Path.GetFileName(gameFile.Path);

                string relativeFilePathInSourceFolder = gameFile.Path.Substring(prefixToSearch.Length);
                string outPath = Path.Combine(currentOutputDirectory, relativeFilePathInSourceFolder.Replace("/", Path.DirectorySeparatorChar.ToString()));

                // Ensure subdirectory for the file exists (e.g. if UI/SubFolder/WB_File.uasset)
                Directory.CreateDirectory(Path.GetDirectoryName(outPath)!);

                if (asJson) // Corresponds to GetFolderAsJson command
                {
                    // Apply "WB_" prefix filter and check for .uasset/.umap extension
                    if (fileName.StartsWith("WB_", StringComparison.OrdinalIgnoreCase) &&
                        (fileName.EndsWith(".uasset", StringComparison.OrdinalIgnoreCase) ||
                         fileName.EndsWith(".umap", StringComparison.OrdinalIgnoreCase)))
                    {
                        Console.WriteLine($"[INFO] Processing for JSON export (WB_ filter matched): {gameFile.Path}");
                        try
                        {
                            var package = provider.LoadPackage(gameFile);
                            if (package == null)
                            {
                                Console.WriteLine($"[WARNING] Failed to load package: {gameFile.Path}");
                                continue;
                            }

                            var allExports = package.GetExports();
                            if (allExports == null || !allExports.Any())
                            {
                                Console.WriteLine($"[INFO] No exports found in package: {gameFile.Path}");
                                continue;
                            }

                            var settings = new JsonSerializerSettings
                            {
                                Formatting = Formatting.Indented
                            };
                            // If CUE4Parse.JsonUtils.MinimalConverters is confirmed available and beneficial:
                            // settings.Converters = CUE4Parse.JsonUtils.MinimalConverters.ToList();

                            string jsonContent = JsonConvert.SerializeObject(allExports, settings);

                            File.WriteAllText(outPath + ".json", jsonContent);
                            Console.WriteLine($"[SUCCESS] Exported JSON: {outPath}.json)");
                            processedCount++;
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine($"[ERROR] Exporting {gameFile.Path} to JSON: {e.Message}\n{e.StackTrace}");
                        }
                    }
                    // else: If asJson is true but it's not a WB_ uasset/umap, we skip it.
                    // No raw extraction for non-matching files when asJson is true.
                }
                else // This is for GetFolder (raw extraction) - extract all files regardless of WB_
                {
                    if (provider.TrySaveAsset(gameFile.Path, out var data))
                    {
                        File.WriteAllBytes(outPath, data);
                        Console.WriteLine($"Extracted raw file: {outPath}");
                        processedCount++;
                    }
                    else
                    {
                        Console.WriteLine($"[ERROR] Error extracting raw file (found in folder, but read failed): {gameFile.Path}");
                    }
                }
            }
            Console.WriteLine($"[INFO] Finished processing folder '{folderPath}'. Processed {processedCount} matching files.");
        }
    }
}