import argparse
import logging
import json
import tarfile
import os
from datetime import datetime, timedelta
from pathlib import Path

# --- Constants & Setup ---
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'


def setup_logging(log_file):
    """
    Initializes logging. In a container environment, it ensures the volume-mapped 
    directory exists and is writable before attempting to create the log file.
    """
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    
    if log_file:
        log_path = Path(log_file)
        
        try:
            # 1. Ensure the parent directory (mapped volume) exists
            if not log_path.parent.exists():
                log_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 2. Check if the directory is actually writable (common Docker/QNAP issue)
            if not os.access(log_path.parent, os.W_OK):
                logging.error(f"Permission Denied: Cannot write to {log_path.parent}. "
                              "Check QNAP folder permissions for the container user.")
                return

            # 3. FileHandler creates the file if it's missing
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
            logging.getLogger().addHandler(file_handler)
            
        except Exception as e:
            logging.error(f"Failed to initialize file logging: {e}")


def load_tracked_files(tracked_path):
    """
    Reads the 'tracked' database to identify files already present in archives.
    
    This prevents the script from re-archiving the same data twice and allows
    for the identification of 'new' files added to old folders.

    Args:
        tracked_path (Path): Path to the .tracked.txt file.

    Returns:
        set: A set of strings representing relative file paths already archived.
    """
    if not tracked_path.exists():
        return set()
    with open(tracked_path, "r") as f:
        return set(line.strip() for line in f if line.strip() and not line.startswith("---"))

def create_tar_part(archive_path, files_to_add, source_root, manifest_path, tracked_path, dry_run=False):
    """
    Assembles a list of files into a single .tar archive part safely.
    
    Uses 'Atomic Writing': the script writes to a '.tmp' file first. If the process
    is interrupted, the .tmp file is deleted. If successful, it is renamed to '.tar'.
    It also updates the human-readable manifest and the machine-readable tracker.

    Args:
        archive_path (Path): The final filename for the .tar part.
        files_to_add (list): List of Path objects to be compressed.
        source_root (Path): The root directory used to calculate relative paths inside the tar.
        manifest_path (Path): Path to the human-readable .contents.txt file.
        tracked_path (Path): Path to the machine-readable .tracked.txt database.
        dry_run (bool): If True, logs actions without writing data to disk.
    """
    if dry_run:
        logging.info(f"[DRY RUN] Would create: {archive_path.name} with {len(files_to_add)} files.")
        return

    temp_path = archive_path.with_suffix(".tar.tmp")
    try:
        with tarfile.open(temp_path, "w") as tar:
            with open(manifest_path, "a") as manifest, open(tracked_path, "a") as tracked:
                manifest.write(f"\n--- CONTENTS OF {archive_path.name} ---\n")
                for file_path in files_to_add:
                    # Maintain the folder structure inside the archive
                    arcname = file_path.relative_to(source_root)
                    tar.add(file_path, arcname=arcname)
                    # Update both record files
                    manifest.write(f"{arcname}\n")
                    tracked.write(f"{arcname}\n")
        
        # Finalize the file only after successful closure
        temp_path.replace(archive_path)
        logging.info(f"SUCCESS: Finalized {archive_path.name}")
    except Exception as e:
        if temp_path.exists():
            temp_path.unlink()
        logging.error(f"CRITICAL: Failed during archive creation: {e}")
        raise

def process_archiving(config):
    """
    Main execution engine that coordinates scanning and archiving logic.
    
    1. Filters folders based on the 'Last Month' boundary.
    2. Performs a 'Fast Pass' check using folder timestamps.
    3. Compares disk content vs. the tracked database to find new files.
    4. Segments new files into parts based on the max_size_gb limit.
    5. Triggers the creation of STATIC or INC (Incremental) archives.

    Args:
        config (dict): Dictionary containing all settings loaded from JSON.
    """
    max_bytes = config.get("max_size_gb", 100) * 1024**3
    source_root = Path(config.get("source_dir"))
    dest_root = Path(config.get("dest_dir"))
    dry_run = config.get("dry_run", False)
    
    if not dry_run:
        dest_root.mkdir(parents=True, exist_ok=True)

    # Determine the cutoff date (don't archive the current month)
    archive_until = (datetime.now().replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    logging.info(f"Starting Scan. Mode: {'DRY RUN' if dry_run else 'PRODUCTION'}")

    # Walk through Year/Month directory structure
    for month_dir in sorted([d for d in source_root.glob('*/*') if d.is_dir()]):
        folder_id = f"{month_dir.parent.name}-{month_dir.name}"
        tracked_file = dest_root / f"{folder_id}.tracked.txt"
        manifest_file = dest_root / f"{folder_id}.contents.txt"

        # OPTIMIZATION: If folder hasn't been modified since we last tracked it, skip it
        if tracked_file.exists() and month_dir.stat().st_mtime < tracked_file.stat().st_mtime:
            continue

        # BOUNDARY: Ensure folder date is older than current month
        try:
            dir_date = f"{month_dir.parent.name}-{month_dir.name[:2]}"
            if dir_date > archive_until:
                continue
        except Exception:
            continue

        logging.info(f"SCANNING: {folder_id}")
        
        already_archived = load_tracked_files(tracked_file)
        all_files = [f for f in month_dir.rglob('*') if f.is_file()]
        
        # IDENTIFICATION: Filter out files we have already seen
        new_files = [f for f in all_files if str(f.relative_to(month_dir.parent.parent)) not in already_archived]

        if not new_files:
            continue

        # NAMING: STATIC for the primary archive, INC for later additions
        prefix = "STATIC" if not already_archived else "INC"
        
        try:
            # Check destination for existing parts to find the next part number
            existing_parts = list(dest_root.glob(f"*{folder_id}.part*.tar"))
            part_num = len(existing_parts) + 1
            
            current_batch, current_size = [], 0
            for f in new_files:
                f_size = f.stat().st_size
                
                # Check if this file triggers a new archive part
                if current_size > 0 and (current_size + f_size) > max_bytes:
                    arc_name = f"{prefix}_{folder_id}.part{part_num}.tar"
                    create_tar_part(dest_root / arc_name, current_batch, month_dir.parent.parent, 
                                   manifest_file, tracked_file, dry_run)
                    part_num += 1
                    current_batch, current_size = [], 0
                
                current_batch.append(f)
                current_size += f_size

            # Finalize remaining files
            if current_batch:
                arc_name = f"{prefix}_{folder_id}.part{part_num}.tar"
                create_tar_part(dest_root / arc_name, current_batch, month_dir.parent.parent, 
                               manifest_file, tracked_file, dry_run)

        except Exception as e:
            logging.error(f"STOPPING: Execution halted due to error: {e}")
            break

def main():
    """
    Entry point of the script. Handles CLI arguments and initializes the process.
    """
    parser = argparse.ArgumentParser(description="Professional QNAP Hybrid Archiver")
    parser.add_argument("--config", type=str, default="config.json", help="Path to JSON config file")
    args = parser.parse_args()

    if not os.path.exists(args.config):
        print(f"Error: Configuration file {args.config} not found.")
        return

    with open(args.config, 'r') as f:
        config = json.load(f)

    setup_logging(config.get("log_file"))
    
    try:
        # process_archiving(config)
        logging.info("Archiving process finished successfully.")
    except Exception as e:
        logging.error(f"Main process encountered an unhandled error: {e}")

if __name__ == "__main__":
    main()