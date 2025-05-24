import os
import subprocess
import time
import logging
from datetime import datetime
from pathlib import Path

# Configure logging
log_dir = Path(__file__).parent / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"master_script_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('MasterScript')

def run_script(script_name, description=None):
    """Run a Python script and log the results"""
    start_time = time.time()
    desc = description or f"Running {script_name}"
    logger.info(f"Starting: {desc}")
    
    try:
        # Run the script as a subprocess
        result = subprocess.run(['python', script_name], 
                              check=True, 
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.PIPE,
                              text=True)
        
        # Log the output
        if result.stdout:
            logger.info(f"{script_name} output:\n{result.stdout}")
        
        elapsed_time = time.time() - start_time
        logger.info(f"Completed: {desc} in {elapsed_time:.2f} seconds")
        return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing {script_name}: {e}")
        logger.error(f"Error output: {e.stderr}")
        return False
    
    except Exception as e:
        logger.error(f"Unexpected error running {script_name}: {e}")
        return False

def main():
    """Main function to run all scripts in sequence"""
    logger.info("Starting master script workflow")
    
    # Step 1: Run ytword.py to extract YouTube data
    if not run_script('ytword.py', 'Extracting YouTube data from Word documents'):
        logger.error("YouTube data extraction failed. Stopping workflow.")
        return False
    
    # Step 2: Run cleandata.py to clean Excel files
    if not run_script('cleandata.py', 'Cleaning Excel data files'):
        logger.error("Data cleaning failed. Continuing to analysis with caution.")
        # Continuing anyway as analysis might still work with uncleaned data
    
    # Step 3: Run genv2.py to generate reports
    if not run_script('genv2.py', 'Generating brand analytics reports'):
        logger.error("Report generation failed.")
        return False
    
    logger.info("Master script workflow completed successfully")
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)