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
import win32com.client
import re
import sys

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
                "archive_processed": True,
                "brand_detection_columns": ["brand", "account", "profile", "channel"],
                "hashtag_detection_columns": ["hashtag", "tag", "keyword"],
                "post_detection_columns": ["post", "content", "caption"],
                "global_detection_terms": ["overview", "global", "summary", "total"]
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

    def standardize_excel_files(self, file_paths):
        """
        Use VBA to standardize the Excel files for processing
        """
        self.logger.info("Starting Excel file standardization...")
        
        vba_script = """
Sub StandardizeSheets(brandName)
    Dim ws As Worksheet
    
    ' Disable alerts to avoid confirmation dialogs
    Application.DisplayAlerts = False
    
    ' Loop through all worksheets
    For Each ws In ActiveWorkbook.Worksheets
        ' Check content and rename sheet based on purpose
        If WorksheetContainsTerms(ws, Array("hashtag", "tag", "#")) Then
            ws.Name = brandName & " Hashtag Usage"
        ElseIf WorksheetContainsTerms(ws, Array("post", "content", "caption")) Then
            ws.Name = brandName & " Post Information"
        ElseIf WorksheetContainsTerms(ws, Array("overview", "summary")) Then
            ws.Name = "Overview"
        ElseIf WorksheetContainsTerms(ws, Array("global", "metrics", "total")) Then
            ws.Name = "Global Metrics"
        End If
    Next ws
    
    ' Re-enable alerts
    Application.DisplayAlerts = True
    
    MsgBox "Sheets standardized for brand: " & brandName, vbInformation
End Sub

Function WorksheetContainsTerms(ws As Worksheet, terms As Variant) As Boolean
    Dim term As Variant
    Dim cell As Range
    Dim searchRange As Range
    
    ' Try to determine a reasonable search range (first few rows and columns)
    On Error Resume Next
    Set searchRange = ws.Range("A1:J10")
    On Error GoTo 0
    
    ' Check sheet name first
    For Each term In terms
        If InStr(1, ws.Name, term, vbTextCompare) > 0 Then
            WorksheetContainsTerms = True
            Exit Function
        End If
    Next term
    
    ' Check content of cells in search range
    If Not searchRange Is Nothing Then
        For Each cell In searchRange.Cells
            If Not IsEmpty(cell) Then
                For Each term In terms
                    If InStr(1, cell.Value, term, vbTextCompare) > 0 Then
                        WorksheetContainsTerms = True
                        Exit Function
                    End If
                Next term
            End If
        Next cell
    End If
    
    WorksheetContainsTerms = False
End Function
        """
        
        try:
            # Initialize Excel application
            excel = win32com.client.Dispatch("Excel.Application")
            excel.Visible = False  # Run Excel in background
            
            # Process each file
            for file_path in file_paths:
                if file_path and file_path.exists():
                    # Determine brand name from file or content
                    brand_name = self.detect_brand_from_file(file_path)
                    if not brand_name:
                        self.logger.warning(f"Could not determine brand name for {file_path}. Using filename.")
                        brand_name = file_path.stem.split('_')[0]  # Use first part of filename
                    
                    # Open workbook
                    self.logger.info(f"Standardizing {file_path} for brand {brand_name}")
                    workbook = excel.Workbooks.Open(str(file_path))
                    
                    # Add VBA module and run standardization
                    excel.VBE.ActiveVBProject.VBComponents.Add(1)  # 1 = vbext_ct_StdModule
                    module = excel.VBE.ActiveVBProject.VBComponents.Item(excel.VBE.ActiveVBProject.VBComponents.Count)
                    module.CodeModule.AddFromString(vba_script)
                    
                    # Run the standardization macro
                    excel.Run("StandardizeSheets", brand_name)
                    
                    # Save and close workbook
                    workbook.Save()
                    workbook.Close()
            
            # Quit Excel
            excel.Quit()
            
            self.logger.info("Excel file standardization complete.")
            return True
        
        except Exception as e:
            self.logger.error(f"Error standardizing Excel files: {e}")
            # Make sure Excel is closed if there's an error
            try:
                if 'excel' in locals():
                    excel.Quit()
            except:
                pass
            return False
    
    def detect_brand_from_file(self, file_path):
        """
        Attempt to detect brand name from Excel file content
        """
        try:
            xl = pd.ExcelFile(file_path)
            
            # Look for brand name in sheet names first
            for sheet_name in xl.sheet_names:
                # Check if sheet name contains brand name indicators
                match = re.search(r'([A-Z][a-z]+)\s+(Hashtag|Post|Overview)', sheet_name)
                if match:
                    return match.group(1)  # Return the brand name part
            
            # If not found in sheet names, try to find in content
            # Check for columns that might contain brand name
            brand_columns = self.config.get("brand_detection_columns", ["brand", "account", "profile", "channel"])
            
            for sheet_name in xl.sheet_names:
                df = pd.read_excel(xl, sheet_name=sheet_name)
                
                # Check column names
                for col in df.columns:
                    for brand_col in brand_columns:
                        if brand_col.lower() in col.lower():
                            # If we found a brand column, get the most frequent non-empty value
                            values = df[col].dropna().value_counts()
                            if len(values) > 0:
                                return values.index[0]
            
            # If still not found, try to guess from filename
            filename = file_path.stem
            # Look for CamelCase or words separated by underscores
            match = re.search(r'([A-Z][a-z]+|[^_]+)(?=_|$)', filename)
            if match:
                return match.group(1)
                
            return None
        
        except Exception as e:
            self.logger.error(f"Error detecting brand from file {file_path}: {e}")
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

    def extract_brand_names(self, excel_files):
        """Extract all unique brand names from the Excel files"""
        brand_names = set()
        
        for file_path in excel_files:
            if file_path and file_path.exists():
                try:
                    xl = pd.ExcelFile(file_path)
                    
                    # Look for brand names in sheet names
                    for sheet_name in xl.sheet_names:
                        # Check common naming patterns like "Brand X Hashtag Usage"
                        match = re.search(r'([A-Z][a-z]+)\s+(Hashtag|Post|Information)', sheet_name)
                        if match:
                            brand_names.add(match.group(1))
                    
                    # If no brands found in sheet names, try to find them in data
                    if not brand_names:
                        brand_name = self.detect_brand_from_file(file_path)
                        if brand_name:
                            brand_names.add(brand_name)
                
                except Exception as e:
                    self.logger.error(f"Error extracting brand names from {file_path}: {e}")
        
        return list(brand_names)

    def get_brand_data(self, excel_data, brand_name):
        """Extract specific brand data from the Excel data structure"""
        brand_data = {}
        
        # First try exact match in sheet names
        for sheet_name, data in excel_data.items():
            if brand_name in sheet_name:
                brand_data[sheet_name] = data
        
        # If no exact matches, try to determine by content
        if not brand_data:
            for sheet_name, data in excel_data.items():
                if data and len(data) > 0:
                    # Check if first few records might contain the brand name
                    sample_records = data[:min(5, len(data))]
                    for record in sample_records:
                        for field, value in record.items():
                            if isinstance(value, str) and brand_name.lower() in value.lower():
                                brand_data[sheet_name] = data
                                break
        
        # Add global data that's relevant to all brands
        global_terms = self.config.get("global_detection_terms", ["overview", "global", "summary", "total"])
        for sheet_name, data in excel_data.items():
            for term in global_terms:
                if term.lower() in sheet_name.lower():
                    brand_data[sheet_name] = data
                    break
                
        return brand_data

    def categorize_brand_data(self, brand_data):
        """Categorize brand data into logical groups (hashtags, posts, etc.)"""
        categorized_data = {
            "hashtags": {},
            "posts": {},
            "overview": {},
            "other": {}
        }
        
        hashtag_terms = self.config.get("hashtag_detection_columns", ["hashtag", "tag", "keyword"])
        post_terms = self.config.get("post_detection_columns", ["post", "content", "caption"])
        global_terms = self.config.get("global_detection_terms", ["overview", "global", "summary", "total"])
        
        for sheet_name, data in brand_data.items():
            # Categorize by sheet name first
            if any(term.lower() in sheet_name.lower() for term in hashtag_terms):
                categorized_data["hashtags"][sheet_name] = data
            elif any(term.lower() in sheet_name.lower() for term in post_terms):
                categorized_data["posts"][sheet_name] = data
            elif any(term.lower() in sheet_name.lower() for term in global_terms):
                categorized_data["overview"][sheet_name] = data
            else:
                # If can't determine by sheet name, try to analyze content
                categorized_data["other"][sheet_name] = data
                
                # Check column names to see if we can categorize
                if data and len(data) > 0:
                    # Get the keys from the first record
                    columns = data[0].keys()
                    
                    if any(any(term.lower() in col.lower() for term in hashtag_terms) for col in columns):
                        categorized_data["hashtags"][sheet_name] = data
                        del categorized_data["other"][sheet_name]
                    elif any(any(term.lower() in col.lower() for term in post_terms) for col in columns):
                        categorized_data["posts"][sheet_name] = data
                        del categorized_data["other"][sheet_name]
        
        return categorized_data

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
            
        # Categorize the data to make it more useful
        categorized_data = self.categorize_brand_data(data)
        
        platform_prompt = f"""
        MOST IMPORTANT RULE: DO NOT SHOW ANY THINKING OR REASONING PROCESS in the generated markdown report. ONLY PROVIDE THE FINAL REPORT IN MARKDOWN FORMAT.

        You are a comprehensive marketing analyst for the brand {brand_name}. 
        Analyze the provided {platform} data and search the web to generate a detailed brand report specific to {platform}.

        AVAILABLE DATA:
        Hashtag Data: {json.dumps(categorized_data["hashtags"], indent=2) if categorized_data["hashtags"] else "No specific hashtag data available"}
        
        Post Information: {json.dumps(categorized_data["posts"], indent=2) if categorized_data["posts"] else "No specific post data available"}
        
        Overview Data: {json.dumps(categorized_data["overview"], indent=2) if categorized_data["overview"] else "No overview data available"}
        
        Other Data: {json.dumps(categorized_data["other"], indent=2) if categorized_data["other"] else "No other data available"}

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
        MOST IMPORTANT RULE: DO NOT SHOW ANY THINKING OR REASONING PROCESS in the markdown report. ONLY PROVIDE THE FINAL REPORT IN MARKDOWN FORMAT.

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
        brand_youtube_data = self.get_brand_data(youtube_data, brand_name)
        
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
                    archive_path = self.archive_dir / f"{platform}_{file.name}_{timestamp}"
                    shutil.copy2(file, archive_path)
                    self.logger.info(f"Archived {file} to {archive_path}")
            
            self.logger.info("All processed files archived successfully.")
    
    def run(self):
        """Main processing pipeline"""
        try:
            self.logger.info("Starting Brand Analytics processing...")
            
            # Find latest files
            insta_file, fb_file, youtube_file = self.find_latest_files()
            
            # Standardize the Excel files to make processing easier
            self.standardize_excel_files([insta_file, fb_file, youtube_file])
            
            # Load data from Excel files
            insta_data = self.load_excel_data(insta_file) if insta_file else {}
            fb_data = self.load_excel_data(fb_file) if fb_file else {}
            youtube_data = self.load_excel_data(youtube_file) if youtube_file else {}
            
            # Extract all brand names
            brand_names = self.extract_brand_names([insta_file, fb_file, youtube_file])
            
            if not brand_names:
                self.logger.warning("No brand names detected. Attempting to use filenames.")
                # Fallback to using filenames
                for file in [insta_file, fb_file, youtube_file]:
                    if file:
                        brand_names.append(file.stem.split('_')[0])
                
                # Remove duplicates
                brand_names = list(set(brand_names))
            
            self.logger.info(f"Detected brands: {', '.join(brand_names)}")
            
            # Process each brand
            brand_reports = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.get("max_workers", 3)) as executor:
                # Submit brand processing tasks
                future_to_brand = {
                    executor.submit(self.process_brand, brand, insta_data, fb_data, youtube_data): brand
                    for brand in brand_names
                }
                
                # Collect results as they complete
                for future in concurrent.futures.as_completed(future_to_brand):
                    brand = future_to_brand[future]
                    try:
                        report_path = future.result()
                        if report_path:
                            brand_reports.append(report_path)
                            self.logger.info(f"Completed processing for brand: {brand}")
                        else:
                            self.logger.error(f"Failed to process brand: {brand}")
                    except Exception as e:
                        self.logger.error(f"Error processing brand {brand}: {e}")
            
            # Generate comparative report
            if len(brand_reports) > 1:
                comparative_report = self.generate_comparative_report(brand_names, brand_reports)
                if comparative_report:
                    self.logger.info(f"Comparative report generated at {comparative_report}")
            else:
                self.logger.info("Skipping comparative report as only one brand was processed.")
            
            # Archive processed files
            if self.config.get("archive_processed", True):
                self.archive_processed_files(insta_file, fb_file, youtube_file)
            
            self.logger.info("Brand Analytics processing completed successfully.")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in main processing pipeline: {e}")
            return False
    
    def export_to_html(self, output_dir=None):
        """Convert all markdown reports to HTML for better presentation"""
        try:
            from markdown import markdown
            from bs4 import BeautifulSoup
            
            output_dir = output_dir or self.output_dir
            html_dir = output_dir / "html"
            html_dir.mkdir(exist_ok=True)
            
            self.logger.info(f"Exporting reports to HTML in {html_dir}")
            
            # Find all markdown files
            md_files = list(output_dir.glob("**/*.md"))
            
            # Create HTML template
            html_template = """<!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>{title}</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        line-height: 1.6;
                        max-width: 1200px;
                        margin: 0 auto;
                        padding: 20px;
                        color: #333;
                    }}
                    h1, h2, h3, h4 {{
                        color: #2c3e50;
                    }}
                    table {{
                        border-collapse: collapse;
                        width: 100%;
                        margin: 20px 0;
                    }}
                    th, td {{
                        padding: 12px;
                        border: 1px solid #ddd;
                        text-align: left;
                    }}
                    th {{
                        background-color: #f2f2f2;
                    }}
                    tr:nth-child(even) {{
                        background-color: #f9f9f9;
                    }}
                    .container {{
                        padding: 20px;
                        background-color: #fff;
                        box-shadow: 0 0 10px rgba(0,0,0,0.1);
                    }}
                    .nav {{
                        margin-bottom: 20px;
                        background-color: #f8f9fa;
                        padding: 10px;
                    }}
                    a {{
                        color: #3498db;
                        text-decoration: none;
                    }}
                    a:hover {{
                        text-decoration: underline;
                    }}
                    code {{
                        background-color: #f8f9fa;
                        padding: 2px 4px;
                        border-radius: 4px;
                    }}
                    blockquote {{
                        border-left: 4px solid #e7e9eb;
                        padding-left: 15px;
                        color: #666;
                    }}
                </style>
            </head>
            <body>
                <div class="nav">
                    <a href="index.html">Home</a> |
                    {nav_links}
                </div>
                <div class="container">
                    {content}
                </div>
            </body>
            </html>
            """
            
            # Generate navigation links
            nav_links = []
            for md_file in md_files:
                html_filename = md_file.stem + ".html"
                report_name = " ".join(word.capitalize() for word in md_file.stem.split('_'))
                nav_links.append(f'<a href="{html_filename}">{report_name}</a>')
            
            nav_html = " | ".join(nav_links)
            
            # Create index page
            index_content = f"""
            <h1>Brand Analytics Reports</h1>
            <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <h2>Available Reports:</h2>
            <ul>
                {"".join(f'<li><a href="{md_file.stem}.html">{" ".join(word.capitalize() for word in md_file.stem.split("_"))}</a></li>' for md_file in md_files)}
            </ul>
            """
            
            # Create index.html
            index_html = html_template.format(
                title="Brand Analytics Reports",
                nav_links=nav_html,
                content=index_content
            )
            
            with open(html_dir / "index.html", "w", encoding='utf-8') as f:
                f.write(index_html)
            
            # Convert each markdown file to HTML
            for md_file in md_files:
                try:
                    # Read markdown content
                    with open(md_file, "r", encoding='utf-8') as f:
                        md_content = f.read()
                    
                    # Convert to HTML
                    html_content = markdown(md_content, extensions=['tables', 'fenced_code'])
                    
                    # Clean HTML with BeautifulSoup
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    # Add CSS classes
                    for table in soup.find_all('table'):
                        table['class'] = 'table'
                    
                    # Get HTML as string
                    html_body = str(soup)
                    
                    # Create final HTML
                    title = " ".join(word.capitalize() for word in md_file.stem.split('_'))
                    final_html = html_template.format(
                        title=title,
                        nav_links=nav_html,
                        content=html_body
                    )
                    
                    # Write HTML file
                    with open(html_dir / f"{md_file.stem}.html", "w", encoding='utf-8') as f:
                        f.write(final_html)
                    
                    self.logger.info(f"Converted {md_file} to HTML")
                
                except Exception as e:
                    self.logger.error(f"Error converting {md_file} to HTML: {e}")
            
            self.logger.info(f"All reports exported to HTML in {html_dir}")
            return html_dir
            
        except ImportError:
            self.logger.error("Required packages for HTML export not found. Install with: pip install markdown beautifulsoup4")
            return None
        except Exception as e:
            self.logger.error(f"Error exporting to HTML: {e}")
            return None

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Brand Analytics Processor')
    parser.add_argument('--config', '-c', help='Path to configuration file')
    parser.add_argument('--export-html', '-e', action='store_true', help='Export reports to HTML')
    parser.add_argument('--input-dir', '-i', help='Override input directory')
    parser.add_argument('--output-dir', '-o', help='Override output directory')
    parser.add_argument('--brands', '-b', nargs='+', help='Specify brands to process (comma-separated)')
    parser.add_argument('--api-key', '-k', help='Perplexity API key (overrides config file and environment variable)')
    return parser.parse_args()

def main():
    """Main function"""
    args = parse_arguments()
    
    try:
        # Initialize processor
        processor = BrandAnalyticsProcessor(config_path=args.config)
        
        # Override settings with command line arguments
        if args.input_dir:
            processor.input_dir = Path(args.input_dir)
            processor.logger.info(f"Input directory overridden to {processor.input_dir}")
            
        if args.output_dir:
            processor.output_dir = Path(args.output_dir)
            processor.logger.info(f"Output directory overridden to {processor.output_dir}")
            
        if args.api_key:
            processor.api_key = args.api_key
            processor.logger.info("API key overridden by command line argument")
            
        # Run processing
        success = processor.run()
        
        # Export to HTML if requested
        if args.export_html and success:
            processor.export_to_html()
    
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)