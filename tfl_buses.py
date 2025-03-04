import requests
import csv
from datetime import datetime
import re

# API Call for Bus Line Status
print("Fetching bus line status...")
url_status = "https://api.tfl.gov.uk/Line/Mode/bus/Status"
response_status = requests.get(url_status)

if response_status.status_code == 200:
    data_status = response_status.json()
    print("Bus line status fetched successfully.")
else:
    print(f"Error fetching bus line status: {response_status.status_code}")
    data_status = []

# Function to get the stops for a given bus line
def get_line_route(line_id):
    print(f"Fetching route for bus line: {line_id}...")
    url_route = f"https://api.tfl.gov.uk/Line/{line_id}/StopPoints"
    response_route = requests.get(url_route)

    if response_route.status_code == 200:
        try:
            data_route = response_route.json()  # Parse JSON response
            print(f"Route for bus line {line_id} fetched successfully.")
            
            # Extracting stop names
            if isinstance(data_route, list):  # Ensure data is in the expected list format
                stops = [stop["commonName"] for stop in data_route]
                return stops
            else:
                print(f"Unexpected response format for line {line_id}: {data_route}")
                return []
        except ValueError as e:
            print(f"Error parsing JSON response for bus line {line_id}: {e}")
            return []
    else:
        print(f"Error fetching route for bus line {line_id}: {response_route.status_code}")
        return []

# CSV File Name
csv_file = "tfl_realtime_data_buses.csv"

# Prepare data rows
rows = []
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Regex pattern to extract delay time (in minutes) from reason description
delay_pattern = r"(\d+)\s?minute[s]?\s?delay"

for line in data_status:
    line_name = line["name"]
    line_id = line["id"]  # Get line ID for fetching its stops
    print(f"Processing bus line: {line_name} (ID: {line_id})")
    
    # Fetching the route (stops) for the current line
    stops = get_line_route(line_id)
    stops_str = ", ".join(stops)  # Joining stop names into a single string
    
    for status in line["lineStatuses"]:
        status_description = status["statusSeverityDescription"]
        reason = status.get("reason", "No Delay")  # Get reason or default to "No delay"
        print(f"Status for {line_name}: {status_description} - {reason}")
        
        # Check if the reason includes a delay time (using regex)
        delay_time = "N/A"
        match = re.search(delay_pattern, reason, re.IGNORECASE)
        if match:
            delay_time = match.group(1)  # Extracted delay time in minutes
            print(f"Delay time detected: {delay_time} minutes")
        
        # Adding the row with stops and delay information
        rows.append([timestamp, line_name, status_description, reason, delay_time, stops_str])

# Writing data to CSV
print("Writing data to CSV file...")
with open(csv_file, mode="a", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    
    # Write header if the file is empty
    if file.tell() == 0:
        writer.writerow(["Timestamp", "Line", "Status", "Reason", "Delay Time (Minutes)", "Route (Stops)"])
    
    # Write data rows
    writer.writerows(rows)

print(f"Data successfully saved to {csv_file}")

