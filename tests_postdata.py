from data_object import PostData
from datetime import datetime

test_post_add_new_row = PostData(
    id=99999,
    url="https://geekhack.org/index.php?topic=99999",
    title="Test Posting",
    author="tester",
    replies=123456,
    reply_timestamp=datetime(year=2022, month=1, day=1, hour=0, minute=0, second=0),
    reply_author="also_tester",
    first_seen=datetime(year=2022, month=1, day=1, hour=0, minute=0, second=0)
)