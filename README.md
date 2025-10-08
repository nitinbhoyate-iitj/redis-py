# Redis Client

A Redis client implementation using `redis-py` for cloud database operations.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Start local Redis server:
```bash
redis-server
```

3. (Optional) Update connection details in `connect()` method if needed:
```python
self.redis = redis.Redis(
    host='localhost',
    port=6379,
    db=0,
    decode_responses=True
)
```

## Run

```python
python redis_client.py
```

## Data Structures

- **Hash**: `user:<id>` - User attributes
- **Sorted Set**: `leaderboard:<id>` - User scores
- **Search Index**: `user_index` - Complex queries

## Methods

- `connect()` - Connect to Redis
- `load_users(file)` - Load user data from CSV
- `load_scores(file)` - Load score data from CSV
- `query1(usr)` - Get user attributes
- `query2(usr)` - Get user coordinates
- `query3()` - Get users with even-numbered IDs
- `query4()` - Get female users in China/Russia (lat 40-46)
- `query5()` - Get top 10 player emails from leaderboard:2