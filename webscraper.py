from bs4 import BeautifulSoup
import requests
import re
import pyodbc
import logging

# Logging
logging.basicConfig(filename='scraper.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# User-Agent Header
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

# Database
def setup_database():
    try:
        conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=127.0.0.1, 1433;DATABASE=wtsordained;UID=SA;PWD=dockerStrongPwd123'
        conn = pyodbc.connect(conn_str)
        logging.info("Database connection established.")
        return conn
    except pyodbc.Error as e:
        logging.error(f"Database connection error: {e}")
        raise

# Insert data into the database
def save_to_database(conn, church_name, staff_info):
    try:
        cursor = conn.cursor()
        for info in staff_info:
            cursor.execute('''INSERT INTO Ordained (ChurchName, Name, Degree, Seminary)
                            VALUES(?, ?, ?, ?)''', (church_name, info['name'], info['degree'], info['seminary']))
        conn.commit()
        logging.info(f"Data saved for {church_name}.")
    except pyodbc.Error as e:
        logging.error(f"Database error while inserting data for {church_name}: {e}")
        conn.rollback()
    except Exception as e:
        logging.error(f"Unexpected error in save_to_database: {e}")
        conn.rollback()

# Fetch the list of church URLs from the directory
def fetch_church_urls(directory_url):
    try:
        response = requests.get(directory_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract URLs from the directory page
        church_links = []
        for tr in soup.find_all('tr'):
            cells = tr.find_all('td')
            if len(cells) > 0:
                for cell in cells:
                    a_tag = cell.find('a', href=True)
                    if a_tag and 'Website' in cell.text:
                        church_links.append(a_tag['href'])

        logging.info(f"Found {len(church_links)} church URLs.")
        return church_links
    except requests.RequestException as e:
        logging.error(f"Request error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error in fetch_church_urls: {e}")
        raise

# Extract staff page URL from each church's website
def find_staff_page(church_url):
    try:
        response = requests.get(church_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find links that might point to staff page
        for a in soup.find_all('a', href=True):
            if any(keyword in a['href'].lower() for keyword in ['staff', 'team', 'leadership', 'pastors']):
                staff_page_url = a['href']
                if not staff_page_url.startswith('http'):
                    staff_page_url = requests.compat.urljoin(church_url, staff_page_url)
                return staff_page_url

        logging.info(f"No staff page found for {church_url}.")
        return None
    except requests.RequestException as e:
        logging.error(f"Request error for {church_url}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error in find_staff_page: {e}")
        return None

# Extract information about pastors and elders
def extract_staff_info(staff_url):
    try:
        response = requests.get(staff_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Example extraction logic
        staff_info = []
        for person in soup.find_all('div', class_='staff-member'):
            name = person.find('h3').text.strip()
            degrees = person.find('p', class_='degrees').text.strip()
            seminary = re.findall(r'from\s(.*)', degrees)

            staff_info.append({
                'name': name,
                'degree': degrees,
                'seminary': seminary[0] if seminary else 'Unknown'
            })

        logging.info(f"Extracted information for {len(staff_info)} staff members from {staff_url}.")
        return staff_info
    except requests.RequestException as e:
        logging.error(f"Request error for {staff_url}: {e}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error in extract_staff_info: {e}")
        return []

# Main script
def main():
    directory_url = 'https://presbyteryportal.pcanet.org/ac/directory'

    try:
        conn = setup_database()
        church_urls = fetch_church_urls(directory_url)

        for church_url in church_urls:
            staff_page_url = find_staff_page(church_url)
            if staff_page_url:
                staff_info = extract_staff_info(staff_page_url)
                save_to_database(conn, church_url, staff_info)

    except Exception as e:
        logging.error(f"Error in main script: {e}")
    finally:
        try:
            conn.close()
            logging.info("Database connection closed.")
        except NameError:
            logging.warning("Database connection was never established.")

if __name__ == '__main__':
    main()