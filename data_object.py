import sqlite3
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

@dataclass
class PostData:
    """Structure for scraped forum post"""
    id: int
    url: str
    title: str
    author: str
    replies: int
    reply_timestamp: datetime
    reply_author: str
    first_seen: Optional[datetime] = None


class BoardStorage:
    """
    Class for storing and managing posts from a specific board in an SQLite database.
    Handles automatic conversion of DATETIME fields.
    """
    def __init__(self, table_name: str, db_path: str = "hackwatch.db"):
        """Initializes the storage configuration."""
        self.db_path = Path(db_path)
        self.table_name = table_name
        self.conn = None
        self.table_dict = None
        # Columns to check for updates against a PostData object
        self.columns_post = ['url', 'title', 'author', 'replies', 'reply_timestamp', 'reply_author']

    def __enter__(self):
        """
        Establishes the database connection as a context manager.
        - `detect_types=sqlite3.PARSE_DECLTYPES`: Crucial for converting
          database types (like DATETIME) to Python types (datetime.datetime).
        - `row_factory = sqlite3.Row`: Allows accessing columns by name.
        """
        self.conn = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        self.conn.row_factory = self._postdata_factory
        self._init_db()
        self.table_dict = self._table_to_dict()
        return self

    def _postdata_factory(self, cursor, row):
        '''Custom row factoru function to keep timestamps from breaking'''
        field_names = [col[0] for col in cursor.description]
        row_dict = dict(zip(field_names, row))

        # Only include keys that PostData accepts
        relevant_keys = {'id', 'url', 'title', 'author', 'replies', 'reply_timestamp', 'reply_author'}
        filtered_row = {k: v for k, v in row_dict.items() if k in relevant_keys}

        # Convert timestamp from 'YYYY-MM-DD HH:MM:SS'
        filtered_row["reply_timestamp"] = datetime.strptime(filtered_row["reply_timestamp"], "%Y-%m-%d %H:%M:%S")

        return PostData(**filtered_row)



    def __exit__(self, exc_type, exc_val, exc_tb):
        """Commits changes and closes the connection upon exit."""
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def _init_db(self):
        """
        Initialises the database table with the correct schema.
        Note that timestamp columns are declared as DATETIME.
        """
        schema = f'''CREATE TABLE IF NOT EXISTS {self.table_name} (
            id INTEGER PRIMARY KEY NOT NULL,
            url TEXT NOT NULL,
            title TEXT NOT NULL,
            author TEXT NOT NULL,
            replies INTEGER DEFAULT 0,
            reply_timestamp DATETIME,
            reply_author TEXT,
            first_seen DATETIME,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
            read_by_scan BOOLEAN DEFAULT 0
        );
        '''
        self.conn.execute(schema)
        self.conn.commit()

    def _table_to_dict(self):
        """
        Fetches all table contents and converts them into a dictionary
        keyed by the primary key ('id'). Assumes a row_factory that returns
        PostData objects.
        """
        cursor = self.conn.execute(
            f"SELECT id, url, title, author, replies, reply_timestamp, reply_author FROM {self.table_name}"
        )

        table_dict = {}
        for post in cursor.fetchall():  # Each row is a PostData object now
            pk_value = post.id
            table_dict[pk_value] = asdict(post)

        return table_dict
    
    def save_or_update_row(self, post: PostData):
        """
        Saves a new post or updates an existing one by comparing it
        with the in-memory dictionary.
        Returns True if no update was needed (post unchanged), False if post was new or updated.
        """
        # Using a try-except block is slightly cleaner than checking for key existence
        try:
            existing_post = self.table_dict[post.id]
            # Post already exists. Check if any data is updated.
            needs_update = False
            update_clauses = []
            update_values = []
            
            for col in self.columns_post:
                new_value = getattr(post, col)
                # This comparison now works correctly for all types, including datetime.
                if new_value != existing_post[col]:
                    needs_update = True
                    print(f'Update needed for post "{post.title}" on column "{col}":')
                    print(f'  - FROM: {existing_post[col]} (type: {type(existing_post[col])})')
                    print(f'  - TO:   {new_value} (type: {type(new_value)})')
                    update_clauses.append(f"{col} = ?")
                    update_values.append(new_value)
        
            if needs_update:
                # Also update the 'last_updated' timestamp
                update_clauses.append("last_updated = CURRENT_TIMESTAMP")
                update_values.append(post.id)  # For the WHERE clause
            
                query = f"UPDATE {self.table_name} SET {', '.join(update_clauses)} WHERE id = ?"
                self.conn.execute(query, tuple(update_values))
                print(f'Updated existing post: "{post.title}"')
                
                # Update the in-memory dictionary with new values
                for col in self.columns_post:
                    self.table_dict[post.id][col] = getattr(post, col)
                
                return False  # Post was updated
            else:
                print(f'No update needed for post: "{post.title}"')
                return True  # No update was needed
                
        except KeyError:
            # Data does not exist in table_dict (new post). Insert it.
            # print(f'New post found: "{post.title}". Inserting into database.')
            self.conn.execute(
                f'''INSERT INTO {self.table_name}
                    (id, url, title, author, replies, reply_timestamp, reply_author, first_seen)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                (post.id, post.url, post.title, post.author, post.replies, post.reply_timestamp, post.reply_author, post.first_seen)
            )
            
            # Add the new post to the in-memory dictionary
            self.table_dict[post.id] = {
                col: getattr(post, col) for col in self.columns_post
            }
            
            return False  # New post was added


    def try_sqlite_stuff(self, post: PostData):
        '''test function'''
        # test 1 - trying update row/insert row
        # conclusion - loading the full table to a dict is faster by far (1/5th lesser time per page)
        res = self.conn.execute(f"select id, title from {self.table_name} where id = {post.id}")
