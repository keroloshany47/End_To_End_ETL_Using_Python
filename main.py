import subprocess
import sys

# List of scripts to run in order
scripts = [
    "Extraction.py",
    "Quality_check.py",
    "Transformation.py",
    "Modeling.py",
    "Visualization.py"
]

for script in scripts:
    print(f"\n=== Running {script} ===")
    result = subprocess.run([sys.executable, script], capture_output=True, text=True)
    
    # Print script output
    print(result.stdout)
    if result.stderr:
        print(f"Errors in {script}:\n{result.stderr}")
    
    # Stop execution if a script fails
    if result.returncode != 0:
        print(f"{script} failed. Stopping execution.")
        break
else:
    print("\nAll scripts ran successfully!")
