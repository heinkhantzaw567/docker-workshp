from pathlib import Path

current_dir = Path.cwd()
current_file = Path(__file__).name

print(f"Files in {current_dir}:")

for filepath in current_dir.iterdir():
    if filepath.name == current_file:
        continue
    if filepath.is_file():
        print(f"  [FILE] {filepath.name} ({filepath.stat().st_size} bytes)")
        print(f"    Content: {filepath.read_text()}")
    elif filepath.is_dir():
        print(f"  [DIR]  {filepath.name}")

