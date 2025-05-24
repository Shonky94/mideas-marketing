import os

folders = [
    "data/input/instagram",
    "data/input/facebook",
    "data/input/youtube",
    "data/output",
    "config",
    "logs"
]

base_path = "C:\\Users\\Admin\\Desktop\\Final"

for folder in folders:
    folder_path = os.path.join(base_path, folder)
    os.makedirs(folder_path, exist_ok=True)
    print(f"Created: {folder_path}")
