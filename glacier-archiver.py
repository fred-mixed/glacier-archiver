import argparse
import logging
import os
import tarfile
from datetime import datetime, timedelta
from pathlib import Path

# --- Configuration ---
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

def setup_logging(log_file):
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logging.getLogger().addHandler(file_handler)

def load_tracked_files(tracked_path):
    """Loads a set of already archived filenames for a specific month."""
    if not tracked_path.exists():
        return set()
    with open(tracked_path, "r") as f:
        return set(line.strip() for line in f if line.strip() and not line.startswith("---"))

def create_tar_part(archive_path, files_to_add, source_root, manifest_path, tracked_path, dry_run=False):
    """Creates a tar and updates both the human manifest and the machine tracking file."""
    if dry_run:
        logging.info(f"[DRY RUN] Would create: {archive_path.name}")
        return

    temp_path = archive_path.with_suffix(".tar.tmp")
    try:
        with tarfile.open(temp_path, "w") as tar:
            with open(manifest_path, "a") as manifest, open(tracked_path, "a") as tracked:
                manifest.write(f"\n--- CONTENTS OF {archive_path.name} ---\n")
                for file_path in files_to_add:
                    arcname = file_path.relative_to(source_root)
                    tar.add(file_path, arcname=arcname)
                    manifest.write(f"{arcname}\n")
                    tracked.write(f"{arcname}\n")
        
        temp_path.replace(archive_path)
        logging.info(f"SUCCESS: Finalized {archive_path.name}")
    except Exception as e:
        if temp_path.exists(): temp_path.unlink()
        logging.error(f"CRITICAL: Error creating part: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Hybrid Static/Incremental Archiver")
    parser.add_argument("max_size_gb", type=int)
    parser.add_argument("source_dir", type=str)
    parser.add_argument("dest_dir", type=str)
    parser.add_argument("log_file", type=str)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    setup_logging(args.log_file)
    max_bytes = args.max_size_gb * 1024**3
    source_root = Path(args.source_dir)
    dest_root = Path(args.dest_dir)
    if not args.dry_run: dest_root.mkdir(parents=True, exist_ok=True)
    
    archive_until = (datetime.now().replace(day=1) - timedelta(days=1)).strftime("%Y-%m")

    for month_dir in sorted([d for d in source_root.glob('*/*') if d.is_dir()]):
        folder_id = f"{month_dir.parent.name}-{month_dir.name}"
        tracked_file = dest_root / f"{folder_id}.tracked.txt"
        manifest_file = dest_root / f"{folder_id}.contents.txt"

        # Logic check: If no files have changed in the folder since our last track, skip scanning
        if tracked_file.exists() and month_dir.stat().st_mtime < tracked_file.stat().st_mtime:
            continue

        dir_date = f"{month_dir.parent.name}-{month_dir.name[:2]}"
        if dir_date > archive_until: continue

        logging.info(f"SCANNING: {folder_id} for new files...")
        
        # Load database of what we already have
        already_archived = load_tracked_files(tracked_file)
        all_files = [f for f in month_dir.rglob('*') if f.is_file()]
        
        # Find ONLY files that are not in our tracking database
        new_files = [f for f in all_files if str(f.relative_to(month_dir.parent.parent)) not in already_archived]

        if not new_files:
            continue

        prefix = "STATIC" if not already_archived else "INC"
        logging.info(f"ACTION: Found {len(new_files)} new files in {folder_id}. Creating {prefix} archive.")

        try:
            # Find the next part number by checking existing files
            existing_parts = list(dest_root.glob(f"*{folder_id}.part*.tar"))
            part_num = len(existing_parts) + 1
            
            current_batch, current_size = [], 0
            for f in new_files:
                f_size = f.stat().st_size
                if current_size > 0 and (current_size + f_size) > max_bytes:
                    name = f"{prefix}_{folder_id}.part{part_num}.tar"
                    create_tar_part(dest_root / name, current_batch, month_dir.parent.parent, manifest_file, tracked_file, args.dry_run)
                    part_num += 1
                    current_batch, current_size = [], 0
                current_batch.append(f)
                current_size += f_size

            if current_batch:
                name = f"{prefix}_{folder_id}.part{part_num}.tar"
                create_tar_part(dest_root / name, current_batch, month_dir.parent.parent, manifest_file, tracked_file, args.dry_run)

        except Exception as e:
            logging.error(f"STOPPING: {e}")
            break

if __name__ == "__main__":
    main()