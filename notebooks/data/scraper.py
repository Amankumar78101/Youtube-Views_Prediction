import os
import time
import requests
import pandas as pd
import sys

# Define a class called YouTubeDataScraper to manage the scraping process.
class YouTubeDataScraper:
    # Initialize the class with necessary parameters.
    def __init__(self, api_key_path, country_code_path, output_dir):
        # Read and store the API key from the given file path.
        self.api_key = self.read_api_key(api_key_path)
        # Read and store the country codes from the provided file path.
        self.country_codes = self.read_country_codes(country_code_path)
        # Set the output directory for storing the scraped data.
        self.output_dir = output_dir
        # Define a list of features to be extracted from the YouTube video snippet.
        self.snippet_features = ["title", "publishedAt", "channelId", "channelTitle", "categoryId"]
        # Define a list of characters that should be avoided due to potential issues.
        self.unsafe_characters = ['\n', '"']
        # Define the header for the data to be saved, including various attributes.
        self.header = ["video_id"] + self.snippet_features + ["trending_date", "tags", "view_count", "likes",
                                                              "dislikes", "comment_count", "thumbnail_link",
                                                              "comments_disabled", "ratings_disabled", "description"]

    def read_api_key(self, api_path):
        # This function reads an API key from the specified file path.
        # It expects the file to contain the API key as the first line.
        
        # Opens the specified file in read ('r') mode using a context manager.
        with open(api_path, 'r') as file:
            # Reads the first line of the file, which is assumed to contain the API key.
            return file.readline()
        
        
    def read_country_codes(self, code_path):
        # This function reads country codes from a file located at 'code_path'.
        # It opens the file in a 'with' block to ensure proper resource management
        # and automatic closing of the file after reading.
        with open(code_path) as file:
            # Using a list comprehension to process each line (country code) in the file.
            # 'x.rstrip()' is used to remove any trailing newline characters or whitespace.
            # The result is a list containing the cleaned country codes.
            return [x.rstrip() for x in file]

    def prepare_feature(self, feature):
        # Iterate through each unsafe character and replace it with an empty string.
        for ch in self.unsafe_characters:
            feature = str(feature).replace(ch, "")
        
        # Return the sanitized feature enclosed in double quotes.
        return f'"{feature}"'

    def api_request(self, page_token, country_code):
        # Construct the API request URL with necessary parameters
        request_url = f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,snippet{page_token}" \
                    f"chart=mostPopular&regionCode={country_code}&maxResults=50&key={self.api_key}"
        
        # Send a GET request to the constructed URL
        request = requests.get(request_url)
        
        # Check if the request was rate-limited (HTTP 429)
        if request.status_code == 429:
            print("Temp-Banned due to excess requests, please wait and continue later")
            sys.exit()  # Exit the program if rate-limited
            
        # Return the JSON response from the API
        return request.json()
    def get_tags(self, tags_list):
        """
        Takes a list of tags and prepares a feature using them.

        Args:
            tags_list (list): A list of tags to be joined.

        Returns:
            feature (str): A prepared feature string created by joining the tags using a pipe ('|') separator.
        """
        return self.prepare_feature("|".join(tags_list))

    def get_videos(self, items):
        # Initialize an empty list to store the lines of data for each video.
        lines = []

        # Iterate through each video in the 'items' list.
        for video in items:
            # Initialize variables to track whether comments and ratings are disabled for the current video.
            comments_disabled = False
            ratings_disabled = False

            # Check if the video has 'statistics' information, otherwise skip to the next video.
            if "statistics" not in video:
                continue

            # Extract the video ID and prepare it as a feature.
            video_id = self.prepare_feature(video['id'])

            # Extract the 'snippet' and 'statistics' dictionaries from the video.
            snippet = video['snippet']
            statistics = video['statistics']

            # Prepare selected features from the 'snippet' dictionary.
            features = [self.prepare_feature(snippet.get(feature, "")) for feature in self.snippet_features]

            # Extract video description, thumbnail link, and format trending date.
            description = snippet.get("description", "")
            thumbnail_link = snippet.get("thumbnails", dict()).get("default", dict()).get("url", "")
            trending_date = time.strftime("%y.%d.%m")

            # Get and prepare video tags.
            tags = self.get_tags(snippet.get("tags", ["[none]"]))

            # Extract view count from 'statistics'.
            view_count = statistics.get("viewCount", 0)

            # Extract like and dislike counts from 'statistics'.
            likes = statistics.get("likeCount", 0)
            dislikes = statistics.get("dislikeCount", 0)

            # Check if the 'commentCount' key exists in 'statistics'.
            if 'commentCount' in statistics:
                # If it exists, extract the comment count.
                comment_count = statistics['commentCount']
            else:
                # If it doesn't exist, mark comments as disabled and set comment count to 0.
                comments_disabled = True
                comment_count = 0

            # Create a list containing various video-related data for the current video.
            line = [video_id] + features + [self.prepare_feature(x) for x in
                                            [trending_date, tags, view_count, likes, dislikes,
                                            comment_count, thumbnail_link, comments_disabled,
                                            ratings_disabled, description]]

            # Join the list into a comma-separated string and append it to the 'lines' list.
            lines.append(",".join(line))

        # Return the list of formatted lines for all videos.
        return lines
    def get_pages(self, country_code, next_page_token="&"):
        """
        Retrieve video data pages for a given country using YouTube API.

        Args:
            country_code (str): The country code for which video data is requested.
            next_page_token (str, optional): Token for pagination. Defaults to "&".

        Returns:
            list: List of video data for the specified country.

        This method iterates through multiple pages of video data for a given country
        using the YouTube API. It accumulates video data from each page and returns a
        consolidated list of video information.

        Note:
        - This method relies on 'api_request' and 'get_videos' methods from the same class.
        - The 'country_code' parameter is used to specify the target country for the data.
        - The 'next_page_token' parameter is used for pagination. It defaults to '&' for the first page.

        Example usage:
        >> country_videos = get_pages("US")
        >> print(country_videos)  # List of video data for the US country.
        """
        country_data = []
        while next_page_token is not None:

            # Request video data for the current page using the YouTube API.
            video_data_page = self.api_request(next_page_token, country_code)

            # Extract the next page token for pagination, if available.
            next_page_token = video_data_page.get("nextPageToken", None)
            next_page_token = f"&pageToken={next_page_token}&" if next_page_token is not None else next_page_token

            # Extract video items from the current page and add them to the country_data list.
            items = video_data_page.get('items', [])
            country_data += self.get_videos(items)

        return country_data

    def write_to_file(self, country_code, country_data):
        # Function to write country_data to a CSV file for a specific country_code
        
        # Display a message indicating that data is being written to file
        print(f"Writing {country_code} data to file...")

        # Check if the output directory exists; if not, create it
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        # Open a file for writing with a filename based on current time, country code, and format
        with open(f"{self.output_dir}/{time.strftime('%y.%d.%m')}_{country_code}_videos.csv", "w+", encoding='utf-8') as file:
            # Loop through each row in country_data and write it to the file
            for row in country_data:
                file.write(f"{row}\n")



    def get_data(self):
        # This function retrieves data for each country code present in the 'country_codes' list.

        for country_code in self.country_codes:
            # Loop through each country code in the list.

            # Create a list containing the header row as the first element, followed by data retrieved from pages.
            country_data = [",".join(self.header)] + self.get_pages(country_code)

            # Write the collected data for the current country to a file.
            self.write_to_file(country_code, country_data)

    def elist(self, folder_path):
        # Initialize empty lists and dictionary to hold file paths, DataFrame names, and DataFrames
        list0 = []  # List to store file paths
        list1 = []  # List to generate DataFrame names
        list2 = {}  # Dictionary to hold DataFrames with their respective names

        # Get the list of contents (files) in the specified folder
        folder_contents = os.listdir(folder_path)

        # Loop through the files in the folder to create a list of their paths
        for lists in folder_contents:
            file_path = os.path.join(folder_path, lists)
            list0.append(file_path)

        # Generate DataFrame names based on indices and store them in list1
        for i in range(len(list0)):
            list1.append('df' + str(i))

        # Loop through the list of DataFrame names and corresponding file paths
        # Read CSV files and store them in the dictionary list2 with their names as keys
        for i, name in enumerate(list1):
            list2[name] = pd.read_csv(list0[i])

        # Concatenate all DataFrames stored in the dictionary list2 along axis 0 (rows)
        df = pd.concat(list2.values(), axis=0)

        # Return the concatenated DataFrame
        return df

    def list_of_data(self, folder_path):
        # Generate a list of file paths within the specified folder
        file_paths = [os.path.join(folder_path, item) for item in os.listdir(folder_path)]

        # Initialize a dictionary to map country codes to corresponding file paths
        country_code_mapping = {'US': [], 'UK': [], 'GB': [], 'DE': [], 'CA': [], 'FR': [], 'KR': [], 'RU': [],
                                'JP': [], 'BR': [], 'MX': [], 'IN': []}

        # Loop through each file path and assign it to the corresponding country code in the mapping
        for file_path in file_paths:
            for code in country_code_mapping:
                if code in file_path.upper():  # Use upper() to handle case sensitivity
                    country_code_mapping[code].append(file_path)

        # Initialize a dictionary to store dataframes organized by country
        dataframes_by_country = {}

        # Loop through each country code and associated filenames
        for country_code, filenames in country_code_mapping.items():
            # Read each CSV file into a dataframe and store them in a list
            dataframes = [pd.read_csv(filename) for filename in filenames]
            # Concatenate the dataframes along the rows to combine the data
            dataframes_by_country[country_code] = pd.concat(dataframes, axis=0)
        # Return the dictionary containing dataframes organized by country
        return dataframes_by_country

    def save_dataframe(self, save_data_name, folder_path, data):
        """
            Save a pandas DataFrame to a CSV file.

            Args:
                save_data_name (str): Name to be used for the saved CSV file (without extension).
                folder_path (str): Path to the folder where the CSV file will be saved.
                data (pandas.DataFrame): The DataFrame to be saved.
            """
        folder_path = os.path.expanduser(folder_path)  # Expand user-abbreviated paths
        file_path = os.path.join(folder_path, f"{save_data_name}.csv")  # Construct the full file path
        data.to_csv(file_path, index=False)  # Save the DataFrame to CSV format without index

    def run(self):
        # Call the method "get_data" to retrieve some data.
        self.get_data()

        # Define the path to a total folder where data will be saved.
        total_folder_path = r'C:\Users\atulk\PycharmProjects\pythonProject2\M_L\Youtube_Views_Prediction\notebooks\data'
        
        # Save a dataframe named 'Total_data' to the specified folder.
        # The data is obtained from the 'elist' method within the 'output_dir'.
        self.save_dataframe(save_data_name='Total_data', folder_path=total_folder_path,
                            data=self.elist(self.output_dir))
        folder_path=r'C:\Users\atulk\PycharmProjects\pythonProject2\M_L\Youtube_Views_Prediction\notebooks\data\list_of_data'
        # Obtain a dictionary of dataframes categorized by country code.
        dataframes_by_country = self.list_of_data(self.output_dir)
        
        # Iterate through each country code and its corresponding dataframe.
        for country_code, dataframe in dataframes_by_country.items():
            # Save the specific country's dataframe to the total folder.
            self.save_dataframe(save_data_name=country_code, folder_path=folder_path,
                                data=dataframe)

# Entry point of the script
if __name__ == "__main__":
    # Define the file paths for the API key and country codes
    api_key_path = r'C:\Users\atulk\PycharmProjects\pythonProject2\M_L\Youtube_Views_Prediction\notebooks\data\api_key.txt'
    country_code_path = r'C:\Users\atulk\PycharmProjects\pythonProject2\M_L\Youtube_Views_Prediction\notebooks\data\country_codes.txt'

    # Define the output directory path
    output_dir = r'C:\Users\atulk\PycharmProjects\pythonProject2\M_L\Youtube_Views_Prediction\output'

    # Create an instance of YouTubeDataScraper with the specified paths
    scraper = YouTubeDataScraper(api_key_path=api_key_path, country_code_path=country_code_path, output_dir=output_dir)
    
    # Run the YouTube data scraper to collect information
    scraper.run()
