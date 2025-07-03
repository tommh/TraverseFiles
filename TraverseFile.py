import os

def traverse_folder(folder_path):
    """
    Traverse a folder and print all filenames
    """
    try:
        # Walk through the directory tree
        for root, dirs, files in os.walk(folder_path):
            # Print current directory
            print(f"\nDirectory: {root}")
            print("-" * 50)
            
            # Print all files in current directory
            for file in files:
                print(f"  📄 {file}")
                
    except FileNotFoundError:
        print(f"Error: Folder '{folder_path}' not found!")
    except PermissionError:
        print(f"Error: Permission denied to access '{folder_path}'")

# Example usage
if __name__ == "__main__":
    # Change this to your desired folder path
    folder_path = r"C:\EnovaPDF"  # Windows
    
    print(f"Traversing folder: {folder_path}")
    traverse_folder(folder_path)