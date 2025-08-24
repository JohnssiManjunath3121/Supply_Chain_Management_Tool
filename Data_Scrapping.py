import httpx
import time
from typing import Dict
from bs4 import BeautifulSoup
import mysql.connector  # For MySQL database
import re

# Global session for API requests
client = httpx.Client()

# Nexar API Credentials
clientId = "b0f45392-a258-4e9c-9a56-81ec8335613d"
clientSecret = "h4o-TlPsdln0vQhKprq0Kd6iy_YxSYp2Uw3k"  # Replace with actual Secret
NEXAR_URL = "https://api.nexar.com/graphql"
PROD_TOKEN_URL = "https://identity.nexar.com/connect/token"

# MySQL Database Configuration
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = "123456789"
DB_NAME = "supply_chain_db"

# Connect to MySQL database
conn = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME
)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS parts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    mpn VARCHAR(255),
    manufacturer VARCHAR(255),
    seller VARCHAR(255),
    inventory_level INT,
    price DECIMAL(10, 2),
    lead_time VARCHAR(255),
    url TEXT
)
''')
conn.commit()

def get_token(client_id, client_secret):
    """Retrieve Nexar API token using client credentials."""
    if not client_id or not client_secret:
        raise ValueError("client_id and client_secret must be provided.")

    try:
        response = client.post(
            url=PROD_TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            follow_redirects=False,
        )
        response.raise_for_status()  # Raise an error for HTTP failures
        return response.json()
    except httpx.RequestError as e:
        raise Exception(f"Error fetching token: {e}")

def decodeJWT(token):
    """Mock function to decode JWT token. In real cases, use `jwt` module."""
    return {"exp": time.time() + 3600}  # Token expires in 1 hour

class NexarClient:
    token_flag = False

    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.s = httpx.Client()
        self.s.keep_alive = False  # Avoid connection reuse issues
        self.token_data = None  # Store token data

        # Perform token initialization only once
        if not NexarClient.token_flag:
            self.token_data = self.retrieve_or_generate_token()
            self.s.headers.update({"Authorization": f"Bearer {self.token_data.get('access_token')}"})
            self.exp = decodeJWT(self.token_data.get('access_token')).get('exp')
            NexarClient.token_flag = True
        else:
            print("Token already initialized.")

    def retrieve_or_generate_token(self):
        """Retrieve a token or generate a new one if expired."""
        if not self.token_data:
            print("No token found. Generating a new one.")
            self.token_data = get_token(self.client_id, self.client_secret)
        else:
            exp = decodeJWT(self.token_data.get('access_token')).get('exp')
            if exp < time.time() + 300:
                print("Token expired or about to expire. Refreshing...")
                self.token_data = get_token(self.client_id, self.client_secret)

        return self.token_data

    def get_query(self, query: str, variables: Dict) -> dict:
        """Send a GraphQL query to Nexar API and return the response."""
        if NexarClient.token_flag:
            try:
                r = self.s.post(
                    NEXAR_URL,
                    json={"query": query, "variables": variables},
                )
                response = r.json()

                # Handle token expiration
                if r.status_code == 401 or "errors" in response:
                    print("Token validation failed. Refreshing token...")
                    self.token_data = self.retrieve_or_generate_token()
                    self.s.headers.update({"Authorization": f"Bearer {self.token_data.get('access_token')}"})

                    # Retry the request with the new token
                    r = self.s.post(NEXAR_URL, json={"query": query, "variables": variables})
                    response = r.json()

            except Exception as e:
                raise Exception(f"Exception while getting Nexar response: {e}")

            if "errors" in response:
                raise Exception(f"Errors in response: {response['errors']}")

            return response["data"]

def extract_direct_url(octopart_url):
    """Extract direct Mouser or DigiKey URL from Octopart URL."""
    try:
        response = httpx.get(octopart_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the direct URL for Mouser or DigiKey
        for a_tag in soup.find_all('a', href=True):
            if 'mouser.com' in a_tag['href'] or 'digikey.com' in a_tag['href']:
                return a_tag['href']
        
        return octopart_url  # Fallback to Octopart URL if direct URL not found
    except Exception as e:
        print(f"Error extracting direct URL from {octopart_url}: {e}")
        return octopart_url

def fetch_lead_time(url):
    """Fetch lead time from the provided URL."""
    try:
        response = httpx.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Example: Fetching lead time from DigiKey
        if 'digikey' in url:
            lead_time = soup.find(text="Manufacturer Standard Lead Time").find_next().text.strip()
        # Example: Fetching lead time from Mouser
        elif 'mouser' in url:
            lead_time = soup.find(text="Delivery time from manufacturer").find_next().text.strip()
        else:
            lead_time = "N/A"
        
        return lead_time
    except Exception as e:
        print(f"Error fetching lead time from {url}: {e}")
        return "N/A"

def SearchMPN(que):
    gqlQuery = '''
    query SearchMPN($que: String!) {  
      supSearch(
        q: $que        
        start: 0
        limit: 1 
      ) {   
        results {      	
          part {
            mpn
            manufacturer { name }
            sellers(authorizedOnly: true) {   
              company { name }
              offers { 
                inventoryLevel
                prices {                
                  quantity
                  price        
                }                            
                clickUrl    
              }
            }
          }
        }    
      }
    }
    '''

    # Execute the query with the provided query variable
    data = nexar.get_query(gqlQuery, {"que": que})
    return data

def format_and_store_data(part_json_data):
    """Format the data and store it in the database."""
    part = part_json_data['supSearch']['results'][0]['part']
    mpn = part['mpn']
    manufacturer = part['manufacturer']['name']

    for seller in part['sellers']:
        seller_name = seller['company']['name']
        for offer in seller['offers']:
            inventory_level = offer['inventoryLevel']
            price = offer['prices'][0]['price'] if offer['prices'] else 0
            octopart_url = offer['clickUrl']
            
            # Extract direct URL for Mouser or DigiKey
            direct_url = extract_direct_url(octopart_url)
            lead_time = fetch_lead_time(direct_url)

            # Insert data into the MySQL database
            cursor.execute('''
            INSERT INTO parts (mpn, manufacturer, seller, inventory_level, price, lead_time, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (mpn, manufacturer, seller_name, inventory_level, price, lead_time, direct_url))
            conn.commit()

            print(f"Stored data for {mpn} from {seller_name}: Inventory={inventory_level}, Price={price}, Lead Time={lead_time}, URL={direct_url}")

# Create a NexarClient instance
nexar = NexarClient(clientId, clientSecret)
partlist = [
    'CBR02C120F5GAC', 'GRJ188R72A104KE11D',
    'GRM033R61A104ME15D', 'GRM033R61A105ME15D',  
    'GRM033R61A225ME47D', 'GRM033R61C473KE84D',
    'GRM033R71A103KA01D', 'GRM155R60J106ME15D',
    'GRM155R60J475ME47D', 'GRM188R61E475KE11D',
    'GRM21BR61A476ME15L', 'BAS316-TP',
    'LXZ1-PB01', 'VLMS1500-GS08',
    'U.FL-R-SMT-1(10)', 'ADL3225V-470MT-TL000',
    'BLM15HD102SN1D', 'BLM18PG471SN1D',
    'MBKK1608T3R3M', 'MMBT3904LP-7B',
    'ERJ-1GNF1001C', 'ERJ-1GNF1002C',
    'ERJ-1GNF1003C', 'ERJ-1GNF1022C',
    'ERJ-1GNF1801C', 'ERJ-1GNF4752C',
    'ERJ-1GNF49R9C', 'ATmega328-MU',
    'BNO055', 'DS90UB913ATRTVTQ1',
    'LMV321IDCKR', 'LTC3218EDDB#TR',
    'MAX14574EWL+', 'NCP163AMX330TBG',
    'NOIP1SP0480A', 'PCA9509PGM',
    'SIT8008BC-13-33E-66.600000G',
    'TLV7103318DSER', 'TPL0102-100RUCR'
]

try:
    for part in partlist:
        try:
            part_json_data = SearchMPN(part)
            if part_json_data and part_json_data['supSearch']['results']:
                format_and_store_data(part_json_data)
            else:
                print(f"No data found for part: {part}")
        except Exception as e:
            print(f"Error processing part {part}: {e}")
finally:
    # Close the database connection
    cursor.close()
    conn.close()