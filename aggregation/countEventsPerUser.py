# Data: Logs like:

# timestamp, user_id, event_type
# 2026-01-30 12:01:00, u123, click
# 2026-01-30 12:02:01, u456, view
# 2026-01-30 12:03:05, u123, view
# 2026-01-30 12:03:20, u789, click


# Problem: Count how many events each user did over a time window (day/hour).

### memory fitable case can be solve:

from collections import defaultdict

# Dictionary to store counts
user_event_count = defaultdict(int)

# Read line by line (memory efficient for normal logs)
with open("events.log", "r") as file:
    for line in file:
        try:
            timestamp, user_id, event_type = line.strip().split(",")
            user_event_count[user_id] += 1
        except ValueError:
            # Skip malformed lines
            continue

# Print counts
for user_id, count in user_event_count.items():
    print(f"{user_id}: {count}")
