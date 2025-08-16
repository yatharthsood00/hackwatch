'''main'''

from typing import List, Optional
import asyncio
from urllib.parse import urlparse, parse_qs, urlencode
from datetime import datetime
import aiohttp
from bs4 import BeautifulSoup

from data_object import PostData, BoardStorage
from utils import boards


class HackWatch:
    '''Scraping object for one Geekhack board at a time'''

    def __init__(self, board: int):
        self.url = "https://geekhack.org/index.php?board="
        self.board = board
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def scrape_page_text(self, page_url: str) -> List[PostData]:
        '''Delegation of page text data'''
        page_text = await self._get_page_content(page_url)
        if not page_text:
            return []

        soup = BeautifulSoup(page_text, 'html.parser')
        post_rows = self._extract_post_rows(soup)

        posts = []
        for row in post_rows:
            post_data = self._build_postdata(row)
            if post_data:
                posts.append(post_data)

        return posts

    async def get_page_count(self, page_url: str):
        '''
        Getting page count of the board, to set limit for how many pages to read.
        Run outside the class to get the total count of posts needing scraping
        '''
        page_text = await self._get_page_content(page_url)
        if not page_text:
            return

        soup = BeautifulSoup(page_text, 'html.parser')

        pagenav = soup.find('div', class_='pagelinks floatleft')
        # pageList = [p.get_text(strip=True) for p in pagenav]
        pagenav_string = str(pagenav.text.strip())
        pagenav_split = pagenav_string.split()
        pgno_index = pagenav_split.index("»")
        # print(int(pagenav_split[pgno_index-1])*50)
        return int(pagenav_split[pgno_index-1])*50
        # print(pagenav.text.strip())
        # print(pageList[-3])


    async def _get_page_content(self, url: str):
        '''Pvt: Fetch response of one page'''
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    return await response.text()
                print(f"Status {response.status} returned")
                return None
        except Exception as e:
            print(f"Error fetching {url} - {e}")
            return None

    def _extract_post_rows(self, soup: BeautifulSoup) -> List:
        '''Pvt: Extract all post rows from an extracted page'''
        table = soup.find('table', class_='table_grid')
        if not table:
            return []
        return table.find_all('tr', class_=None)

    def _build_postdata(self, row) -> Optional[PostData]:
        '''
        Pvt: Parse a single table row into PostData
        Split into many helper methods for readability
        All helper methods start with "parse" for readability
        '''
        subject_cell = row
        # print(subject_cell)
        try:
            # Extract all components
            title, author, post_id, url = self._parse_title_author_id_url(subject_cell)
            replies = self._parse_replies(row)
            reply_timestamp, reply_author, first_seen = self._parse_reply_time_and_author(row)

            return PostData(
                id=post_id,
                url=url,
                title=title,
                author=author,
                replies=replies,
                reply_timestamp=reply_timestamp,
                reply_author=reply_author,
                first_seen=first_seen
            )
        except Exception as e:
            # Log error but don't crash entire scrape
            if subject_cell.find('td', class_='subject stickybg2'):
                print("Ignoring pinned post")
                return None
            print(f"Error parsing row: {e}")
            return None

    def _parse_title_author_id_url(self, subject_cell) -> tuple[str, str]:
        '''Extract title and author from subject cell'''
        title_cell = subject_cell.find('td', class_='subject windowbg2') or subject_cell.find('td', class_='subject lockedbg2')

        title_span = title_cell.find('span')
        title = title_span.text.strip()

        author_p = title_cell.find('p')
        author_string = author_p.text.splitlines()[0]
        by_index = author_string.find('by') + 2
        author = author_string[by_index:].strip()

        link = title_cell.find('span').find('a', href=True)
        url_full = link['href']

        # Parse and clean URL
        parsed = urlparse(url_full)
        params = parse_qs(parsed.query)
        topic_id = int(float(params['topic'][0]))

        # Build clean URL with only topic parameter
        clean_params = {'topic': topic_id}
        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urlencode(clean_params)}"

        return title, author, topic_id, clean_url

    def _parse_replies(self, row) -> tuple[int, int]:
        '''Extract reply count and view count from stats cell'''
        stats_cell = row.find('td', class_='stats windowbg') or row.find('td', class_='stats lockedbg')
        stats_text = stats_cell.text.strip().lower()
        replies_end = stats_text.find('replies')
        replies = int(stats_text[:replies_end].strip())

        return replies

    def _parse_reply_time_and_author(self, row) -> tuple[datetime, str]:
        '''Extract last post timestamp and author'''
        lastpost_cell = row.find('td', class_='lastpost windowbg2') or row.find('td', class_='lastpost lockedbg2')
        lastpost_text = lastpost_cell.text.strip()

        # Split on 'by' to separate timestamp and author
        by_index = lastpost_text.find('by')
        timestamp_str = lastpost_text[:by_index].strip()
        reply_author = lastpost_text[by_index + 2:].strip()

        reply_timestamp = self._util_convert_timestamp(timestamp_str)

        first_seen = datetime.now()

        return reply_timestamp, reply_author, first_seen

    def _util_convert_timestamp(self, timestamp_string: str):
        '''Internal method to convert GH table timestamp'''
        return datetime.strptime(timestamp_string, "%a, %d %B %Y, %H:%M:%S")


async def try_func(board_name: str, board_data: tuple[int, str]):
    '''test'''
    async with HackWatch(board_data[0]) as scraper:
        base_url = scraper.url + str(scraper.board)

        print("For Board", board_name)

        pagecounts = await scraper.get_page_count(page_url=base_url)

        with BoardStorage(table_name=board_data[1]) as post_saver:

            # Process pages sequentially from 0 to 3000 in increments of 50
            for post_num in range(0, pagecounts, 50):
                # Add delay between requests (except for the first one)
                if post_num > 0:
                    print("Waiting 3 seconds before next request...")
                    await asyncio.sleep(3)

                # Construct the URL for this page
                if post_num == 0:
                    current_url = base_url  # First page has no suffix
                else:
                    current_url = f"{base_url}.{post_num}"

                print(f"\n--- Processing posts from: {current_url} ---")

                try:
                    posts = await scraper.scrape_page_text(current_url)

                    if not posts:
                        print(f"No posts found on page {post_num}. Continuing to next page.")
                        continue

                    print(f"Found {len(posts)} posts on page {post_num}")

                    # Process posts sequentially for this page
                    for post in posts:
                        no_update_needed = post_saver.save_or_update_row(post)
                        # print(f"Status for post '{post.title}': {'No update needed' if no_update_needed else 'Updated/Added'}")

                        if no_update_needed:
                            print(f"Row was not updated/added. Nothing new to update. Exiting at page {post_num}")
                            return
                except Exception as e:
                    print(f"Error processing page {post_num}: {e}")
                    # Continue to next page even if current page fails
                    continue

            print("All pages processed - reached the end without finding unchanged posts")



if __name__ == "__main__":
    for name, data in boards.items():
        asyncio.run(try_func(name, data))
