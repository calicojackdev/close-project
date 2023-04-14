# Customer Support Engineer - Project  

## General Logic  
This script can be used to parse an input file for leads and create them within Close. Since the integrity of the input data is not known, a few steps are taken to clean it up using the popular Python package Pandas:
- Convert dates to a standard format  
- Using a lookup, translate state names to standard abbreviations  
- Validate phone numbers are of the expected length and strip out all other characters aside from digits  
- Uppercase all names  
- Parse incoming emails for a valid structure (ie `address@domain.tld`)  

The script also allows you to query leads in Close by date range and apply a light analysis to the results. Using Close's Advanced Filtering endpoint and handy Visual Query Builder, the script takes in a start and end date (YYYY-MM-DD) and returns the leads founded within that range. After getting the leads, the individual lead endpoint is called to obtain the necessary detail for segmentation (ie lead state, lead revenue). Again using Pandas, the script aggregates leads by state and applies calculations to the revenue values to find the required data. To find the lead with the most revenue in a given state, the data is simply sorted by descending revenue and then grouped by state - with this it can be inferred that the first lead in each state is responsible for the most revenue.  

## Other Considerations
A variety of improvements could be made to this script, including:  
- Exception handling, especially surrounding the API requests  
- Logging of exceptions and unsuccessful lead creations  
- More intelligent rate limit handling. Currently, a half second speedbump is added between individual requests. Exponential backoff could be implemented to help ensure requests are successfully processed in case of reaching a rate limit  
- Email cleaning/validation algo could be optimized, related regex could be enhanced  
- The call to the advanced filter endpoint may be able to return the appropriate data, but I was unable to return the custom fields using the `_fields` key. If this data could be added, the individual calls to get leads may not be necessary and should result in a significant performance improvement.  
- The start/end dates supplied for segmentation could be provided as command line arguments or be supplied from a configuration file. A default value could be added for at least the end date (eg "today").  
- The method used to obtain the lead with the highest revenue in a given state is somewhat indirect and relies on sorting, a more robust solution could be implemented.  

## Running  
1. Clone the repo
2. Install requirements `pip install -r requirements.txt`  
3. Ensure you have set an environment variable called `close_project_api_key` with a valid Close API Key  
4. Update `main.py` with the desired date range for segmentation in L13-14  
5. Run the script `python main.py`  
6. Segmented data will be available in `/output_data/output.csv`  