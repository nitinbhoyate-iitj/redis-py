import csv
import re
from traceback import print_stack
import redis
# https://redis.readthedocs.io/en/stable/examples.html
import sys

class Redis_Client():
    redis = None
    
    def __init__(self):
        self.redis = self.redis
    
    """
    Connect to redis with "host", "port", "db", "username" and "password".
    """
    def connect(self):
        try:
            # Connect to local Redis database
            self.redis = redis.Redis(
                host='localhost',  # Local Redis host
                port=6379,  # Default Redis port
                db=0,  # Default database
                decode_responses=True  # Decode responses to strings
            )
            
            # Test the connection
            if self.redis.ping():
                print("Connected to Redis successfully.")
                return True
            else:
                print("Failed to connect to Redis.")
                return False
        except Exception as e:
            print(f"Connection failed: {e}")
            print_stack()
            return False
    
    """
    Load the users dataset into Redis DB.
    Uses Redis Hash data structure for each user.
    Data structure: user:<user_id> -> Hash with user attributes
    """
    def load_users(self, file):
        result = 0
        try:
            pipe = self.redis.pipeline()
            with open(file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Parse space-separated key-value pairs
                    parts = line.split('"')
                    # Remove empty strings and clean up
                    parts = [part.strip() for part in parts if part.strip()]
                    
                    if len(parts) >= 2:
                        # First part is the user key (e.g., "user:1")
                        user_key = parts[0].strip()
                        user_id = user_key.split(':')[1] if ':' in user_key else user_key
                        
                        # Parse key-value pairs
                        user_data = {}
                        for i in range(1, len(parts), 2):
                            if i + 1 < len(parts):
                                key = parts[i]
                                value = parts[i + 1]
                                user_data[key] = value
                        
                        # Store user data as a hash
                        if user_data:
                            pipe.hset(f"user:{user_id}", mapping=user_data)
                            result += 1
                            
                        # Execute in batches of 100
                        if result % 100 == 0:
                            pipe.execute()
                            pipe = self.redis.pipeline()
            
            # Execute remaining commands
            pipe.execute()
            print(f"Load data for user: {result} users loaded")
        except Exception as e:
            print(f"Error loading users: {e}")
            print_stack()
        return result
    
    """
    Load the scores dataset into Redis DB.
    Uses Redis Sorted Set data structure for leaderboards.
    Data structure: leaderboard:<leaderboard_id> -> Sorted Set with user_id as member and score as score
    """
    def load_scores(self, file):
        result = 0
        try:
            pipe = self.redis.pipeline()
            with open(file, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Extract user_id from "user:id" format
                    user_id_str = row.get('user:id', '')
                    if ':' in user_id_str:
                        user_id = user_id_str.split(':')[1]
                    else:
                        user_id = user_id_str
                    
                    leaderboard_id = row.get('leaderboard')
                    score = float(row.get('score', 0))
                    
                    if leaderboard_id and user_id:
                        # Store score in sorted set
                        pipe.zadd(f"leaderboard:{leaderboard_id}", {user_id: score})
                        result += 1
                        
                        # Execute in batches of 100
                        if result % 100 == 0:
                            pipe.execute()
                            pipe = self.redis.pipeline()
            
            # Execute remaining commands
            pipe.execute()
            print(f"Load data for scores: {result} scores loaded")
        except Exception as e:
            print(f"Error loading scores: {e}")
            print_stack()
        return result
    
    """
    Delete all users in the DB.
    """
    def delete_users(self, hashes):
        pipe = self.redis.pipeline()
        for hash in hashes:
            pipe.delete(hash)
        result = pipe.execute()
        return result
    
    """
    Erase everything in the DB.
    """
    def delete_all(self):
        self.redis.flushdb()
    
    """
    Return all the attribute of the user by usr
    """
    def query1(self, usr):
        print("Executing query 1.")
        try:
            result = self.redis.hgetall(f"user:{usr}")
            if result:
                print(f"User {usr} attributes: {result}")
                return result
            else:
                print(f"No user found with ID: {usr}")
                return None
        except Exception as e:
            print(f"Error in query1: {e}")
            return None
    
    """
    Return the coordinate (longitude and latitude) of the user by the usr.
    """
    def query2(self, usr):
        print("Executing query 2.")
        try:
            coordinates = self.redis.hmget(f"user:{usr}", ['longitude', 'latitude'])
            if all(coordinates):
                print(f"User {usr} coordinates - Longitude: {coordinates[0]}, Latitude: {coordinates[1]}")
                return coordinates
            else:
                print(f"Coordinates not found for user: {usr}")
                return None
        except Exception as e:
            print(f"Error in query2: {e}")
            return None
    
    """
    Get the keys and last names of the users whose ids do not start with an odd number.
    We want to search for a subset of keyspace with the cursor at 1280.
    To avoid the searching of the entire keyspace, we only want to go through only a small number of elements per call.
    """
    def query3(self):
        print("Executing query 3.")
        try:
            userids = []
            result_lastnames = []
            cursor = 1280  # Start cursor at 1280 as specified
            
            while True:
                cursor, keys = self.redis.scan(cursor=cursor, match='user:*', count=100)
                for key in keys:
                    # Extract user ID from key (user:1234 -> 1234)
                    user_id = key.split(':')[1]
                    # Check if ID does not start with odd number (starts with even number or 0)
                    if user_id and int(user_id[0]) % 2 == 0:
                        userids.append(key)
                        last_name = self.redis.hget(key, 'last_name')
                        if last_name:
                            result_lastnames.append(last_name)
                
                # Break if we've scanned all keys (cursor returns to 0)
                if cursor == 0:
                    break
            
            print(f"User IDs not starting with odd number: {userids}")
            print(f"Last names: {result_lastnames}")
            return userids, result_lastnames
        except Exception as e:
            print(f"Error in query3: {e}")
            return [], []
    
    """
    Return the female in China or Russia with the latitude between 40 and 46.
    """
    def query4(self):
        print("Executing query 4.")
        try:
            matching_users = []
            cursor = 0
            
            # Scan through all user keys
            while True:
                cursor, keys = self.redis.scan(cursor=cursor, match='user:*', count=100)
                
                for key in keys:
                    # Get user data
                    user_data = self.redis.hgetall(key)
                    
                    if not user_data:
                        continue
                
                    gender = user_data.get('gender', '')
                    country = user_data.get('country', '')
                    latitude_str = user_data.get('latitude', '')
                    
                    if (gender == 'female' and 
                        country in ['China', 'Russia'] and 
                        latitude_str):
                        try:
                            latitude = float(latitude_str)
                            if 40 <= latitude <= 46:
                                user_info = {
                                    'id': key,
                                    'first_name': user_data.get('first_name', ''),
                                    'last_name': user_data.get('last_name', ''),
                                    'gender': gender,
                                    'country': country,
                                    'latitude': latitude_str,
                                    'longitude': user_data.get('longitude', ''),
                                    'email': user_data.get('email', '')
                                }
                                matching_users.append(user_info)
                                print(f"Found user: {user_info}")
                        except ValueError:
                            # Skip if latitude is not a valid number
                            continue
                
                # Break if we've scanned all keys
                if cursor == 0:
                    break
            
            print(f"Total matching users found: {len(matching_users)}")
            return matching_users
            
        except Exception as e:
            print(f"Error in query4: {e}")
            print_stack()
            return []
    
    """
    Get the email ids of the top 10 players(in terms of score) in leaderboard:2
    """
    def query5(self):
        print("Executing query 5.")
        try:
            # Get top 10 players from leaderboard:2 (sorted set)
            top_players = self.redis.zrevrange('leaderboard:2', 0, 9, withscores=True)
            
            result = []
            for user_id, score in top_players:
                # Get email for each user
                email = self.redis.hget(f"user:{user_id}", 'email')
                if email:
                    result.append(email)
                    print(f"User {user_id} (Score: {score}) - Email: {email}")
            
            print(f"Top 10 player emails: {result}")
            return result
        except Exception as e:
            print(f"Error in query5: {e}")
            return []

# Example usage
if __name__ == "__main__":
    
    rs = Redis_Client()
    rs.connect()
    
    # Clear all data before loading
    rs.delete_all()
    print("All data cleared from Redis database.")
    
    # Load data from provided files
    rs.load_users("users.txt")
    rs.load_scores("userscores.csv")
    
    # Execute queries
    rs.query1(299)
    rs.query2(2836)
    rs.query3()
    rs.query4()
    rs.query5()
