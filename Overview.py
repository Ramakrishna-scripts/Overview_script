import pandas as pd
import csv
from collections import defaultdict
import argparse
import os
import time
from datetime import datetime
import warnings
import re

def write_log(message, log_path):
    print(message)
    with open(log_path, 'a', encoding='utf-8') as log:
        log.write(message + '\n')

def process_csv(file_identity, input_file, output_folder):
    start_time = time.time()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(output_folder, f"process_log_{timestamp}.txt")
    error_log_file = os.path.join(output_folder, f"error_log_{timestamp}.txt")

    def custom_warning_handler(message, category, filename, lineno, file=None, line=None):
        log_msg = f"[WARNING] {filename}:{lineno}: {category.__name__}: {message}"
        write_log(log_msg, log_file)

    warnings.showwarning = custom_warning_handler

    try:
        write_log(f"Starting processing: {input_file}", log_file)

        chunk_size = 100000
        folder_data = defaultdict(lambda: {'size': 0, 'subfolders': set(), 'files': 0})
        server_name = 'Unknown'

        os.makedirs(output_folder, exist_ok=True)

        with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f, delimiter='|', quotechar='"')
            header = next(reader, None)  # skip header if present
            line_num = 1

            chunk = []
            for row in reader:
                line_num += 1
                if not row or len(row) < 3:
                    write_log(f"[SKIPPED] Line {line_num}: Too few columns — line skipped.", error_log_file)
                    continue

                chunk.append((line_num, row))

                if len(chunk) >= chunk_size:
                    process_chunk(chunk, folder_data, server_name, log_file, error_log_file)
                    chunk = []

            # Process final chunk
            if chunk:
                process_chunk(chunk, folder_data, server_name, log_file, error_log_file)

        # Write output
        output_rows = [['"Server_Name"', '"Drive"', '"Top Level Folder"', '"Data(GB)"', '"Number of SubFolders"', '"Number of Files"']]
        for (drive, folder), data in folder_data.items():
            output_rows.append([
                f'"{server_name}"',
                f'"{drive}"',
                f'"{folder}"',
                f'"{round(data["size"] / (1024**3), 2)}"',
                f'"{len(data["subfolders"])}"',
                f'"{data["files"]}"'
            ])

        output_file = os.path.join(output_folder, f'overview_{file_identity}_{timestamp}.csv')
        with open(output_file, 'w', encoding='utf-8') as f:
            for row in output_rows:
                f.write('|'.join(row) + '\n')

        execution_time = round(time.time() - start_time, 2)
        write_log(f"Processing completed in {execution_time} seconds. Output saved at: {output_file}", log_file)
        write_log(f"[{datetime.now()}] Processed: {input_file}, Execution Time: {execution_time} seconds", log_file)
        write_log(f"Log written to: {log_file}", log_file)

    except Exception as e:
        error_message = f"[{datetime.now()}] Error processing {input_file}: {str(e)}"
        write_log("An error occurred. Check the error log for details.", error_log_file)
        write_log(error_message, error_log_file)

def process_chunk(chunk, folder_data, server_name, log_file, error_log_file):
    for line_num, row in chunk:
        try:
            if not server_name or server_name == 'Unknown':
                server_name = row[0].strip()

            full_name = None
            for part in row:
                if '\\' in part and '.' in part:
                    full_name = part.strip()
                    break

            if not full_name:
                write_log(f"[SKIPPED] Line {line_num}: Missing FullName — row skipped.", error_log_file)
                continue

            # Extract DirectoryName by removing the filename
            path_parts = full_name.strip().split('\\')
            if len(path_parts) < 3:
                write_log(f"[SKIPPED] Line {line_num}: FullName malformed — row skipped.", error_log_file)
                continue

            directory = '\\'.join(path_parts[:-1]).strip('\\')

            # Extract drive and top-level folder
            parts = directory.split('\\')
            if len(parts) >= 2:
                drive = f"\\\\{parts[0]}\\{parts[1]}"
                top_level_folder = parts[2] if len(parts) > 2 else 'Not Applicable'
            else:
                drive = directory
                top_level_folder = 'Not Applicable'

            folder_key = (drive, top_level_folder)

            # Extract Length robustly
            length = 0
            for part in row:
                candidate = part.strip('"').strip("'")
                if re.fullmatch(r'\d+', candidate):
                    length = int(candidate)
                    break

            # Extract subfolders beyond the top level
            if len(parts) > 3:
                subfolders = ['\\'.join(parts[3:i+1]) for i in range(3, len(parts))]
                if '.' in parts[-1]:
                    subfolders.pop()  # remove filename from subfolders
                folder_data[folder_key]['subfolders'].update(subfolders)

            folder_data[folder_key]['size'] += length
            folder_data[folder_key]['files'] += 1

        except Exception as e:
            write_log(f"[SKIPPED] Line {line_num}: {e} — row skipped.", error_log_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a large pipe-separated CSV and generate a summary file.")
    parser.add_argument("file_identity", help="A unique identifier for the output file")
    parser.add_argument("input_csv", help="Path to the input CSV file")
    parser.add_argument("output_folder", help="Path to the output folder")

    args = parser.parse_args()
    process_csv(args.file_identity, args.input_csv, args.output_folder)
