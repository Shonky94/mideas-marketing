import os
import pandas as pd
import json
import requests
import argparse
import time
from pathlib import Path
from datetime import datetime
import concurrent.futures
import shutil
import glob
import yaml
import logging
from dotenv import load_dotenv

class BrandAnalyticsProcessor:
    def __init__(self, config_path=None):
        # Set up folder structure
        self.base_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        self.input_dir = self.base_dir / "data" / "input"
        self.output_dir = self.base_dir / "data" / "output"
        self.config_dir = self.base_dir / "config"
        self.log_dir = self.base_dir / "logs"
        
        # Create necessary directories
        for directory in [self.input_dir, self.output_dir, self.config_dir, self.log_dir]:
            directory.mkdir(parents=True, exist_ok=True)
            
        # Set up subdirectories for different data types
        self.insta_dir = self.input_dir / "instagram"
        self.fb_dir = self.input_dir / "facebook"
        self.youtube_dir = self.input_dir / "youtube"
        self.archive_dir = self.input_dir / "archive"
        
        for directory in [self.insta_dir, self.fb_dir, self.youtube_dir, self.archive_dir]:
            directory.mkdir(parents=True, exist_ok=True)
            
        # Set up logging
        self.setup_logging()
        
        # Load configuration
        self.config_path = config_path or self.config_dir / "config.yaml"
        self.load_config()
        
        # Load API key from environment variable or config
        load_dotenv(self.config_dir / ".env")
        self.api_key = os.getenv("PERPLEXITY_API_KEY") or self.config.get("api_key")
        
        if not self.api_key:
            self.logger.error("No API key found. Please set PERPLEXITY_API_KEY environment variable or update config.")
            raise ValueError("API key is required")
        
        # Initialize Perplexity API
        self.perplexity_url = "https://api.perplexity.ai/chat/completions"
        self.perplexity_headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # Track API usage for rate limiting
        self.api_calls = 0
        self.call_timestamps = []
        self.MAX_CALLS_PER_MINUTE = self.config.get("rate_limit", 5)
    
    def setup_logging(self):
        """Set up logging configuration"""
        log_file = self.log_dir / f"brand_analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('BrandAnalytics')
        
    def load_config(self):
        """Load configuration from YAML file or create default if not exists"""
        if not self.config_path.exists():
            # Create default config
            default_config = {
                "rate_limit": 5,
                "max_workers": 3,
                "model": "sonar-deep-research",
                "archive_processed": True
            }
            
            with open(self.config_path, 'w') as f:
                yaml.dump(default_config, f, default_flow_style=False)
                
            self.config = default_config
            self.logger.info(f"Created default configuration at {self.config_path}")
        else:
            # Load existing config
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            self.logger.info(f"Loaded configuration from {self.config_path}")
            
    def save_config(self):
        """Save current configuration to file"""
        with open(self.config_path, 'w') as f:
            yaml.dump(self.config, f, default_flow_style=False)
        self.logger.info(f"Saved configuration to {self.config_path}")
        
    def find_latest_files(self):
        """Find the latest files in each input directory"""
        insta_files = list(self.insta_dir.glob("*.xlsx"))
        fb_files = list(self.fb_dir.glob("*.xlsx"))
        youtube_files = list(self.youtube_dir.glob("*.xlsx"))
        
        latest_insta = max(insta_files, key=lambda x: x.stat().st_mtime) if insta_files else None
        latest_fb = max(fb_files, key=lambda x: x.stat().st_mtime) if fb_files else None
        latest_youtube = max(youtube_files, key=lambda x: x.stat().st_mtime) if youtube_files else None
        
        if not all([latest_insta, latest_fb, latest_youtube]):
            missing = []
            if not latest_insta: missing.append("Instagram")
            if not latest_fb: missing.append("Facebook")
            if not latest_youtube: missing.append("YouTube")
            self.logger.error(f"Missing input files for: {', '.join(missing)}")
        
        return latest_insta, latest_fb, latest_youtube
    
    def manage_rate_limits(self):
        """Manage API rate limits by sleeping if needed"""
        current_time = time.time()
        
        # Remove timestamps older than 1 minute
        self.call_timestamps = [ts for ts in self.call_timestamps if current_time - ts < 60]
        
        # If we've hit the rate limit, sleep until we can make another call
        if len(self.call_timestamps) >= self.MAX_CALLS_PER_MINUTE:
            sleep_time = 60 - (current_time - self.call_timestamps[0]) + 1  # Add 1 second buffer
            self.logger.info(f"Rate limit reached. Waiting {sleep_time:.2f} seconds...")
            time.sleep(max(0, sleep_time))
        
        # Add current call to timestamps
        self.call_timestamps.append(time.time())
        self.api_calls += 1
        
    def query_perplexity(self, prompt, model=None):
        """Send a query to Perplexity API with rate limiting"""
        self.manage_rate_limits()
        
        model = model or self.config.get("model", "sonar-deep-research")
        
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are a marketing expert who provides detailed and analytical responses. Only use information from authorized sources and cite all sources used. Never make up information or estimates."},
                {"role": "user", "content": prompt}
            ]
        }
        
        try:
            response = requests.post(self.perplexity_url, headers=self.perplexity_headers, json=payload)
            response.raise_for_status()
            result = response.json()
            return result["choices"][0]["message"]["content"]
        except Exception as e:
            self.logger.error(f"Error querying Perplexity API: {e}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                self.logger.error(f"Response text: {e.response.text}")
            return None

    def load_excel_data(self, file_path):
        """Load all sheets from an Excel file into a dictionary"""
        try:
            excel_data = {}
            xl = pd.ExcelFile(file_path)
            
            for sheet_name in xl.sheet_names:
                df = pd.read_excel(xl, sheet_name=sheet_name)
                excel_data[sheet_name] = df.to_dict(orient='records')
                
            return excel_data
        except Exception as e:
            self.logger.error(f"Error loading Excel file {file_path}: {e}")
            return None

    def extract_brand_names(self, insta_file_path):
        """Extract all brand names from the Instagram Excel file structure"""
        try:
            xl = pd.ExcelFile(insta_file_path)
            sheet_names = xl.sheet_names
            
            # Brand names are now directly the sheet names, except for the "Competitors Overview"
            brand_names = set()
            
            for sheet in sheet_names:
                # Skip the Competitors Overview sheet
                if "Competitors Overview" not in sheet:
                    brand_names.add(sheet)
            
            return list(brand_names)
        except Exception as e:
            self.logger.error(f"Error extracting brand names: {e}")
            return []

    def get_brand_data(self, excel_data, brand_name):
        """Extract specific brand data from the Excel data structure"""
        brand_data = {}
        
        # Get the sheet that exactly matches the brand name
        if brand_name in excel_data:
            brand_data[brand_name] = excel_data[brand_name]
        
        # Add Competitors Overview data if it exists
        if "Competitors Overview" in excel_data:
            brand_data["Competitors Overview"] = excel_data["Competitors Overview"]
                    
        return brand_data

    def get_youtube_brand_data(self, youtube_data, brand_name):
        """Extract YouTube data for a specific brand"""
        youtube_brand_data = {}
        
        for sheet_name, data in youtube_data.items():
            if sheet_name == brand_name:
                youtube_brand_data[sheet_name] = data
                return youtube_brand_data
            
        # If exact match not found, try partial match
        for sheet_name, data in youtube_data.items():
            if brand_name.lower() in sheet_name.lower():
                youtube_brand_data[sheet_name] = data
                return youtube_brand_data
        
        self.logger.warning(f"No YouTube data found for brand {brand_name}")
        return {}

    def generate_platform_report(self, brand_name, platform, data):
        """Generate a report for a specific brand and platform using Perplexity"""
        self.logger.info(f"Generating {platform} report for {brand_name}...")
        
        # Create report directory
        brand_dir = self.output_dir / brand_name
        brand_dir.mkdir(exist_ok=True)
        
        # Check if report already exists
        report_file = brand_dir / f"{brand_name}_{platform}_report.md"
        
        if report_file.exists():
            self.logger.info(f"{platform} report for {brand_name} already exists. Checking if it needs updating...")
            # You could implement version checking or timestamp comparison here
            # For now, we'll just use the existing file
            
        platform_prompt = f"""
        DO NOT include "<think>" or any thought process in the response. The final report must start directly with "# {brand_name} {platform} Marketing Analysis".


        You are a comprehensive marketing analyst for the brand {brand_name}. 
        Analyze the provided {platform} data and search the web to generate a detailed brand report specific to {platform}.

        AVAILABLE DATA:
        {json.dumps(data, indent=2)}

        TASKS:
        1. BRAND SUMMARY ON {platform.upper()}:
        - Content strategy and patterns on {platform}
        - Posting frequency and engagement levels
        - Visual identity and brand voice
        - Key campaigns identified

        2. CONTENT CATEGORIZATION ON {platform}:
        Categorize content into relevant types for {platform}

        3. HASHTAG STRATEGY:
        - Analyze hashtag usage patterns
        - Identify top performing hashtags
        - Compare to competitor hashtags if data available

        4. CAMPAIGN CLASSIFICATION & ANALYSIS:
        - Identify distinct campaigns on {platform}
        - For each campaign detected:
          * Assign a campaign name
          * List hashtags and CTAs used
          * Performance metrics if available
          * Key themes and messaging

        IMPORTANT GUIDELINES:
        1. DO NOT include any reasoning process in the final report. 
        2. Access the web and research the brand on {platform} to supplement the provided data.
        3. Include ALL important information extracted from the data.
        4. Only cite authorized sources (brand's official accounts, website, annual reports, reputed industry websites).
        5. Do not make up information or estimates.
        6. If you can't determine something with certainty, label it as "Undetermined".

        RESPONSE FORMAT:
        Provide a comprehensive markdown report with the following structure:
        DO NOT include "<think>" or any thought process in the response. The final report must start directly with "# {brand_name} {platform} Marketing Analysis".

        # {brand_name} {platform} Marketing Analysis

        ## 1. Brand Overview on {platform}
        [Detailed brand summary specific to {platform}]

        ## 2. Content Analysis

        ### 2.1 Content Categories
        [List and analysis of content categories]

        ### 2.2 Post Frequency and Timing
        [Analysis of posting patterns]

        ### 2.3 Engagement Analysis
        [Engagement metrics and patterns]

        ## 3. Hashtag Strategy
        [Analysis of hashtag usage]

        ## 4. Campaign Analysis
        
        ### 4.1 Campaign Classifications
        [List of identified campaigns]

        ### 4.2 Detailed Campaign Breakdown
        [Detailed analysis of each campaign]

        ## 5. Recommendations
        [Strategic recommendations based on analysis]

        ---
        *Report generated on {datetime.now().strftime("%Y-%m-%d")} *
        """
        
        try:
            # Skip API call if report already exists
            if not report_file.exists():
                response = self.query_perplexity(platform_prompt)
                
                if response:
                    # Save the platform report
                    with open(report_file, "w", encoding='utf-8') as f:
                        f.write(response)
                    
                    self.logger.info(f"{platform} report for {brand_name} completed and saved.")
                else:
                    self.logger.error(f"Failed to generate {platform} report for {brand_name}.")
                    return None
            else:
                self.logger.info(f"Using existing {platform} report for {brand_name}.")
            
            return report_file
                
        except Exception as e:
            self.logger.error(f"Error in {platform} analysis for {brand_name}: {e}")
            return None

    def merge_brand_reports(self, brand_name, report_files):
        """Merge individual platform reports into a single brand report"""
        self.logger.info(f"Merging reports for {brand_name}...")
        
        brand_dir = self.output_dir / brand_name
        merged_report_path = brand_dir / f"{brand_name}_complete_report.md"
        
        try:
            with open(merged_report_path, "w", encoding='utf-8') as merged_file:
                merged_file.write(f"# {brand_name} Complete Marketing Analysis\n\n")
                merged_file.write(f"*Generated on {datetime.now().strftime('%Y-%m-%d')}*\n\n")
                merged_file.write("---\n\n")
                
                for report_file in report_files:
                    if report_file and report_file.exists():
                        platform = report_file.stem.split('_')[1]  # Extract platform from filename
                        merged_file.write(f"# {platform.upper()} MARKETING ANALYSIS\n\n")
                        
                        with open(report_file, "r", encoding='utf-8') as f:
                            content = f.read()
                            # Remove the header as we've added our own
                            content_lines = content.split('\n')
                            # Find the first line that doesn't start with # or ---
                            start_line = 0
                            for i, line in enumerate(content_lines):
                                if not (line.startswith('#') or line.startswith('---') or line.strip() == ''):
                                    start_line = i
                                    break
                            
                            merged_file.write('\n'.join(content_lines[start_line:]))
                            merged_file.write("\n\n---\n\n")
            
            self.logger.info(f"Merged report for {brand_name} completed and saved.")
            return merged_report_path
            
        except Exception as e:
            self.logger.error(f"Error merging reports for {brand_name}: {e}")
            return None

    def generate_comparative_report(self, brand_names, report_paths):
        """Generate a comparative report of all brands"""
        self.logger.info("Generating comparative report for all brands...")
        
        comparative_file = self.output_dir / "comparative_brand_analysis.md"
        
        # Check if report already exists and is recent
        if comparative_file.exists():
            report_age = datetime.now().timestamp() - comparative_file.stat().st_mtime
            # If report is less than 24 hours old, skip regeneration
            if report_age < 86400:  # 24 hours in seconds
                self.logger.info("Recent comparative report exists. Skipping regeneration.")
                return comparative_file
        
        comparative_prompt = f"""
        DO NOT include "<think>" or any thought process in the response. The final report must start directly with "# {brand_names} Marketing Analysis".


        You are a marketing analytics expert. Create a comprehensive comparative analysis of the following brands:
        {', '.join(brand_names)}

        For each brand, you have the following reports:
        {', '.join([str(path) for path in report_paths if path])}

        TASKS:
        1. COMPARATIVE BRAND POSITIONING:
        - Compare positioning, messaging, and visual identity across brands
        - Identify unique strengths of each brand
        
        2. CONTENT STRATEGY COMPARISON:
        - Compare content types, frequency, and engagement across platforms
        - Identify which brands excel on which platforms
        
        3. HASHTAG STRATEGY COMPARISON:
        - Compare hashtag usage and effectiveness
        - Identify innovative hashtag approaches
        
        4. CAMPAIGN EFFECTIVENESS COMPARISON:
        - Compare campaign approaches and effectiveness
        - Identify standout campaigns across brands
        
        5. CROSS-PLATFORM INTEGRATION:
        - Analyze how well each brand integrates across platforms
        
        6. RECOMMENDATIONS:
        - What can each brand learn from the others?
        - Best practices identified across all brands

        IMPORTANT GUIDELINES:
        1. DO NOT include any reasoning process in the final report. 
        2. Use data from the individual brand reports as your primary source.
        3. Search the web to validate or supplement information when needed.
        4. Do not make up information or estimates.
        5. Use tables and charts in markdown format to highlight key comparisons.

        RESPONSE FORMAT:
        Provide a comprehensive markdown report with the following structure:

        # Comparative Brand Analysis: {', '.join(brand_names)}

        ## 1. Executive Summary
        [High-level comparative summary across all brands]

        ## 2. Brand Positioning Comparison
        [Detailed comparison of brand positioning]

        ## 3. Content Strategy Analysis
        
        ### 3.1 Platform Performance Comparison
        [Analysis of which brands perform best on which platforms]
        
        ### 3.2 Content Type Effectiveness
        [Comparison of content approaches across brands]

        ## 4. Hashtag Strategy Comparison
        [Comparative analysis of hashtag strategies]

        ## 5. Campaign Effectiveness
        [Comparison of campaign approaches and results]

        ## 6. Cross-Platform Integration
        [How well each brand maintains consistency across platforms]

        ## 7. Best Practices & Recommendations
        [Strategic recommendations for each brand]

        ---
        *Comparative Report generated on {datetime.now().strftime("%Y-%m-%d")} *
        """
        
        try:
            response = self.query_perplexity(comparative_prompt)
            
            if response:
                # Save the comparative report
                with open(comparative_file, "w", encoding='utf-8') as f:
                    f.write(response)
                
                self.logger.info("Comparative brand analysis completed and saved.")
                return comparative_file
            else:
                self.logger.error("Failed to generate comparative brand analysis.")
                return None
                
        except Exception as e:
            self.logger.error(f"Error in comparative analysis: {e}")
            return None

    def process_brand(self, brand_name, insta_data, fb_data, youtube_data):
        """Process a single brand's data across all platforms"""
        self.logger.info(f"Processing brand: {brand_name}")
        
        brand_insta_data = self.get_brand_data(insta_data, brand_name)
        brand_fb_data = self.get_brand_data(fb_data, brand_name)
        brand_youtube_data = self.get_youtube_brand_data(youtube_data, brand_name)
        
        # Generate platform-specific reports
        insta_report = self.generate_platform_report(brand_name, "Instagram", brand_insta_data)
        fb_report = self.generate_platform_report(brand_name, "Facebook", brand_fb_data)
        youtube_report = self.generate_platform_report(brand_name, "YouTube", brand_youtube_data)
        
        # Merge reports
        platform_reports = [insta_report, fb_report, youtube_report]
        merged_report = self.merge_brand_reports(brand_name, platform_reports)
        
        return merged_report

    def archive_processed_files(self, insta_file, fb_file, youtube_file):
        """Archive processed files to prevent reprocessing"""
        if self.config.get("archive_processed", True):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            for file, platform in [(insta_file, "instagram"), (fb_file, "facebook"), (youtube_file, "youtube")]:
                if file and file.exists():
                    archive_path = self.archive_dir / platform
                    archive_path.mkdir(exist_ok=True)
                    
                    # Create archive filename with timestamp
                    archived_file = archive_path / f"{timestamp}_{file.name}"
                    
                    # Copy file to archive (don't move to allow for re-runs)
                    shutil.copy2(file, archived_file)
                    self.logger.info(f"Archived {file} to {archived_file}")

    def run_all(self, insta_file=None, fb_file=None, youtube_file=None, max_workers=None):
        """Run the entire brand analytics process for all brands"""
        # Auto-detect files if not specified
        if not all([insta_file, fb_file, youtube_file]):
            insta_file, fb_file, youtube_file = self.find_latest_files()
            
        if not all([insta_file, fb_file, youtube_file]):
            self.logger.error("Missing one or more required input files. Please check input directories.")
            return False
            
        self.logger.info(f"Processing files: \nInstagram: {insta_file}\nFacebook: {fb_file}\nYouTube: {youtube_file}")
            
        # Load all data
        insta_data = self.load_excel_data(insta_file)
        fb_data = self.load_excel_data(fb_file)
        youtube_data = self.load_excel_data(youtube_file)
        
        if not insta_data:
            self.logger.error("Failed to load Instagram data. Aborting.")
            return False
            
        # Extract brand names from Instagram file
        brand_names = self.extract_brand_names(insta_file)
        self.logger.info(f"Found {len(brand_names)} brands: {', '.join(brand_names)}")
        
        if not brand_names:
            self.logger.error("No brands found in the Instagram data. Aborting.")
            return False
        
        # Process each brand (can be parallelized)
        merged_reports = []
        max_workers = max_workers or self.config.get("max_workers", 3)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all brands for processing
            future_to_brand = {
                executor.submit(self.process_brand, brand, insta_data, fb_data, youtube_data): brand
                for brand in brand_names
            }
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_brand):
                brand = future_to_brand[future]
                try:
                    report = future.result()
                    if report:
                        merged_reports.append(report)
                        self.logger.info(f"Successfully processed {brand}")
                    else:
                        self.logger.warning(f"Failed to process {brand}")
                except Exception as e:
                    self.logger.error(f"Exception processing {brand}: {e}")
        
        # Generate comparative report
        if merged_reports:
            comparative_report = self.generate_comparative_report(brand_names, merged_reports)
            if comparative_report:
                self.logger.info("All processing completed successfully.")
                
                # Archive processed files
                self.archive_processed_files(insta_file, fb_file, youtube_file)
                
                return True
        
        self.logger.warning("Processing completed with some errors.")
        return False


def setup_environment():
    """Setup initial environment and folders"""
    base_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    config_dir = base_dir / "config"
    
    # Create config directory if not exists
    config_dir.mkdir(parents=True, exist_ok=True)
    
    # Create .env file if it doesn't exist
    env_file = config_dir / ".env"
    if not env_file.exists():
        # Check if API key is available as environment variable
        api_key = os.environ.get("PERPLEXITY_API_KEY")
        
        if api_key:
            with open(env_file, "w") as f:
                f.write(f"PERPLEXITY_API_KEY={api_key}")
        else:
            # Prompt user for API key
            print("\n===== Brand Analytics Processor =====")
            print("API key not found. You need to provide a Perplexity API key.")
            print("You can set this up in one of two ways:")
            print("1. Enter your API key now (it will be saved to config/.env)")
            print("2. Set the PERPLEXITY_API_KEY environment variable")
            print("3. Add it manually to config/.env file")
            
            user_key = input("\nEnter your Perplexity API key (or press Enter to skip): ").strip()
            
            if user_key:
                with open(env_file, "w") as f:
                    f.write(f"PERPLEXITY_API_KEY={user_key}")
                print("API key saved to config/.env")
            else:
                print("\nNo API key provided. You'll need to:")
                print("- Set the PERPLEXITY_API_KEY environment variable, or")
                print("- Add your API key to config/.env file as PERPLEXITY_API_KEY=your_key_here")
                
    # Display instructions for usage
    print("\n===== Brand Analytics Processor Setup =====")
    print("Folder structure created:")
    print(f"- {base_dir}/data/input/instagram: Place Instagram Excel files here")
    print(f"- {base_dir}/data/input/facebook: Place Facebook Excel files here")
    print(f"- {base_dir}/data/input/youtube: Place YouTube Excel files here")
    print(f"- {base_dir}/data/output: Generated reports will be saved here")
    print(f"- {base_dir}/config: Configuration files")
    print(f"- {base_dir}/logs: Log files")
    print("\nTo use this tool:")
    print("1. Place your Excel files in the appropriate input folders")
    print("2. Run this script to process the latest files")
    print("   python genv2.py")
    print("3. Check the output folder for generated reports\n")


def main():
    parser = argparse.ArgumentParser(description="Process brand analytics data and generate reports")
    parser.add_argument("--insta", help="Path to Instagram analytics Excel file (optional)")
    parser.add_argument("--fb", help="Path to Facebook analytics Excel file (optional)")
    parser.add_argument("--youtube", help="Path to YouTube analytics Excel file (optional)")
    parser.add_argument("--workers", type=int, help="Maximum number of parallel workers")
    parser.add_argument("--setup", action="store_true", help="Setup the environment only")
    parser.add_argument("--config", help="Path to custom config file")

    args = parser.parse_args()

    # Setup the environment if requested or as a first-time initialization
    base_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    if args.setup or not (base_dir / "config").exists():
        setup_environment()
        if args.setup:
            return
    
    try:
        # Initialize the processor
        processor = BrandAnalyticsProcessor(config_path=args.config)
        
        # Convert file paths to Path objects if provided
        insta_file = Path(args.insta) if args.insta else None
        fb_file = Path(args.fb) if args.fb else None
        youtube_file = Path(args.youtube) if args.youtube else None
        
        # Run the processor
        success = processor.run_all(
            insta_file=insta_file, 
            fb_file=fb_file, 
            youtube_file=youtube_file,
            max_workers=args.workers
        )

        if success:
            print("Brand analytics processing completed successfully.")
        else:
            print("Brand analytics processing completed with errors. Check logs for details.")
    
    except Exception as e:
        print(f"Error during processing: {e}")
        logging.error(f"Unhandled exception: {e}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    main()