#!/usr/bin/env python
# coding: utf-8

# In[27]:


import requests

# Define the API URL for retrieving Bitcoin data
url = "https://api.coingecko.com/api/v3/coins/markets"

# Set parameters for the API request
params = {
    'vs_currency': 'usd',  # The currency to retrieve the price in
    'ids': 'bitcoin',      # The cryptocurrency we are interested in
    'order': 'market_cap_desc'  # Order by market capitalization
}

# Make the request to the API
response = requests.get(url, params=params)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Retrieve the data in JSON format
    bitcoin_data = response.json()
     # Add this line to extract the first item from the list
    bitcoin_data = bitcoin_data[0]
    print(bitcoin_data)
else:
    print(f"Error: {response.status_code}")


# In[30]:


#Round all relevant numerical values to 2 decimal places
bitcoin_data['current_price'] = round(bitcoin_data['current_price'], 2)
bitcoin_data['high_24h'] = round(bitcoin_data['high_24h'], 2)
bitcoin_data['low_24h'] = round(bitcoin_data['low_24h'], 2)
bitcoin_data['price_change_24h'] = round(bitcoin_data['price_change_24h'], 2)
bitcoin_data['price_change_percentage_24h'] = round(bitcoin_data['price_change_percentage_24h'], 2)
bitcoin_data['market_cap_change_percentage_24h'] = round(bitcoin_data['market_cap_change_percentage_24h'], 2)
bitcoin_data['ath_change_percentage'] = round(bitcoin_data['ath_change_percentage'], 2)
bitcoin_data['atl_change_percentage'] = round(bitcoin_data['atl_change_percentage'], 2)
print(bitcoin_data)


# In[5]:


# Round all relevant numerical values to 2 decimal places
bitcoin_data['current_price'] = round(bitcoin_data['current_price'], 2)
bitcoin_data['high_24h'] = round(bitcoin_data['high_24h'], 2)
bitcoin_data['low_24h'] = round(bitcoin_data['low_24h'], 2)
bitcoin_data['price_change_24h'] = round(bitcoin_data['price_change_24h'], 2)
bitcoin_data['price_change_percentage_24h'] = round(bitcoin_data['price_change_percentage_24h'], 2)
bitcoin_data['market_cap_change_percentage_24h'] = round(bitcoin_data['market_cap_change_percentage_24h'], 2)
bitcoin_data['ath_change_percentage'] = round(bitcoin_data['ath_change_percentage'], 2)
bitcoin_data['atl_change_percentage'] = round(bitcoin_data['atl_change_percentage'], 2)

# Print the updated data
print(bitcoin_data)


# In[6]:


# Function to format large numbers into millions (M) or billions (B)
def format_large_numbers(num):
    if num >= 1_000_000_000:
        return f"{round(num / 1_000_000_000, 2)}B"
    elif num >= 1_000_000:
        return f"{round(num / 1_000_000, 2)}M"
    else:
        return num

# Apply the function to format the market cap, total volume and market cap change
bitcoin_data['market_cap'] = format_large_numbers(bitcoin_data['market_cap'])
bitcoin_data['total_volume'] = format_large_numbers(bitcoin_data['total_volume'])
bitcoin_data['market_cap_change_24h'] = format_large_numbers(bitcoin_data['market_cap_change_24h'])

# Print the updated data
print(bitcoin_data)


# In[7]:


# Calculate percentage change from the lowest price in the last 24h to the current price
bitcoin_data['percentage_change_from_low'] = round(((bitcoin_data['current_price'] - bitcoin_data['low_24h']) / bitcoin_data['low_24h']) * 100, 2)

# Print the updated data
print(bitcoin_data)


# In[8]:


from datetime import datetime

# Add a timestamp for when the data was retrieved
bitcoin_data['retrieved_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Print the updated data
print(bitcoin_data)


# In[10]:


import sqlite3

# Connect to the SQLite database (or create)
conn = sqlite3.connect('crypto_data.db')

# Create a cursor object to interact with the database
cursor = conn.cursor()

# Create a table to store the Bitcoin data
cursor.execute('''
CREATE TABLE IF NOT EXISTS bitcoin_data (
    id TEXT,
    symbol TEXT,
    name TEXT,
    current_price REAL,
    market_cap TEXT,
    total_volume TEXT,
    high_24h REAL,
    low_24h REAL,
    price_change_24h REAL,
    price_change_percentage_24h REAL,
    market_cap_change_24h TEXT,
    market_cap_change_percentage_24h REAL,
    circulating_supply REAL,
    total_supply REAL,
    max_supply REAL,
    ath REAL,
    ath_change_percentage REAL,
    ath_date TEXT,
    atl REAL,
    atl_change_percentage REAL,
    atl_date TEXT,
    retrieved_at TEXT
)
''')

# Commit the changes and close the connection to the database
conn.commit()


# In[14]:


print("Table created successfully!")


# In[15]:


# Insert the Bitcoin data into the database
cursor.execute('''
INSERT INTO bitcoin_data (
    id, symbol, name, current_price, market_cap, total_volume, high_24h, low_24h, 
    price_change_24h, price_change_percentage_24h, market_cap_change_24h, 
    market_cap_change_percentage_24h, circulating_supply, total_supply, max_supply, 
    ath, ath_change_percentage, ath_date, atl, atl_change_percentage, atl_date, retrieved_at
) 
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
''', (
    bitcoin_data['id'], bitcoin_data['symbol'], bitcoin_data['name'], bitcoin_data['current_price'],
    bitcoin_data['market_cap'], bitcoin_data['total_volume'], bitcoin_data['high_24h'], 
    bitcoin_data['low_24h'], bitcoin_data['price_change_24h'], bitcoin_data['price_change_percentage_24h'],
    bitcoin_data['market_cap_change_24h'], bitcoin_data['market_cap_change_percentage_24h'], 
    bitcoin_data['circulating_supply'], bitcoin_data['total_supply'], bitcoin_data['max_supply'], 
    bitcoin_data['ath'], bitcoin_data['ath_change_percentage'], bitcoin_data['ath_date'], 
    bitcoin_data['atl'], bitcoin_data['atl_change_percentage'], bitcoin_data['atl_date'], 
    bitcoin_data['retrieved_at']
))

# Commit the transaction and close the connection
conn.commit()
conn.close()


# In[16]:


import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('crypto_data.db')

# Create a cursor
cursor = conn.cursor()

# Execute a query to retrieve all data from the bitcoin_data table
cursor.execute("SELECT * FROM bitcoin_data")

# Fetch all the results
rows = cursor.fetchall()

# Print the retrieved data
for row in rows:
    print(row)

# Close the connection
conn.close()


# In[18]:


import logging
import sqlite3
import requests

# Set up logging to log errors into a file
logging.basicConfig(filename='crypto_log.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Fetch data from the CoinGecko API
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'vs_currency': 'usd',
        'ids': 'bitcoin',
        'order': 'market_cap_desc'
    }
    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an exception if the request fails

    bitcoin_data = response.json()[0]

    # Connect to the SQLite database
    conn = sqlite3.connect('crypto_data.db')
    cursor = conn.cursor()

    # Insert data into the database
    cursor.execute('''
    INSERT INTO bitcoin_data (
        id, symbol, name, current_price, market_cap, total_volume, high_24h, low_24h, 
        price_change_24h, price_change_percentage_24h, market_cap_change_24h, 
        market_cap_change_percentage_24h, circulating_supply, total_supply, max_supply, 
        ath, ath_change_percentage, ath_date, atl, atl_change_percentage, atl_date, retrieved_at
    ) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        bitcoin_data['id'], bitcoin_data['symbol'], bitcoin_data['name'], bitcoin_data['current_price'],
        bitcoin_data['market_cap'], bitcoin_data['total_volume'], bitcoin_data['high_24h'], 
        bitcoin_data['low_24h'], bitcoin_data['price_change_24h'], bitcoin_data['price_change_percentage_24h'],
        bitcoin_data['market_cap_change_24h'], bitcoin_data['market_cap_change_percentage_24h'], 
        bitcoin_data['circulating_supply'], bitcoin_data['total_supply'], bitcoin_data['max_supply'], 
        bitcoin_data['ath'], bitcoin_data['ath_change_percentage'], bitcoin_data['ath_date'], 
        bitcoin_data['atl'], bitcoin_data['atl_change_percentage'], bitcoin_data['atl_date'], 
        bitcoin_data['retrieved_at']
    ))

    conn.commit()

# Handle API request errors
except requests.exceptions.RequestException as e:
    logging.error(f"Error fetching data from CoinGecko API: {e}")

# Handle SQLite database errors
except sqlite3.Error as e:
    logging.error(f"SQLite error: {e}")

# Handle any other unexpected errors
except Exception as e:
    logging.error(f"Unexpected error: {e}")

# Ensure the database connection is closed even if an error occurs
finally:
    if 'conn' in locals():
        conn.close()


# In[19]:


print("Data inserted successfully!")


# In[20]:


import unittest

# Create a test case for our functions
class TestCryptoData(unittest.TestCase):
    
    def test_fetch_bitcoin_data(self):
        """Test if the API returns valid Bitcoin data"""
        data = bitcoin_data  # Use the data we have retrieved
        self.assertIn('id', data)  # Check if the 'id' field exists in the data
        self.assertEqual(data['id'], 'bitcoin')  # Check if the id is 'bitcoin'
        self.assertIn('current_price', data)  # Check if the 'current_price' field exists
        
    def test_insert_data(self):
        """Test if data is inserted into the database correctly"""
        conn = sqlite3.connect('crypto_data.db')
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM bitcoin_data")
        rows = cursor.fetchall()
        self.assertGreater(len(rows), 0, "No data found in the database")  # Ensure there is at least one row in the database
        conn.close()

# Run the tests
def run_tests():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCryptoData)
    unittest.TextTestRunner(verbosity=2).run(suite)

# Call the function to run the tests
run_tests()


# In[25]:


import unittest

# Create a test case for error handling
class TestCryptoData(unittest.TestCase):
    
    def test_error_handling(self):
        """Test if errors are handled and printed correctly"""
        
        # Simulate an error and catch it
        try:
            raise ValueError("Test Error")  # Simulating an error
        except ValueError as e:
            print(f"Handled error: {e}")  # Print the error in the notebook console
            self.assertEqual(str(e), "Test Error")  # Verify the error message

# Function to run the tests
def run_tests():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCryptoData)
    unittest.TextTestRunner(verbosity=2).run(suite)

# Call the function to run the tests
run_tests()


# In[26]:


import unittest

# Create a test case for the number formatting function
class TestCryptoData(unittest.TestCase):
    
    def test_format_large_numbers(self):
        """Test if large numbers are formatted correctly"""
        self.assertEqual(format_large_numbers(1_200_000_000), "1.2B")  # Check formatting in billions
        self.assertEqual(format_large_numbers(500_000), 500_000)  # Smaller numbers should remain unchanged
        self.assertEqual(format_large_numbers(3_500_000), "3.5M")  # Check formatting in millions

# Function to run the tests
def run_tests():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCryptoData)
    unittest.TextTestRunner(verbosity=2).run(suite)

# Call the function to run the tests
run_tests()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




