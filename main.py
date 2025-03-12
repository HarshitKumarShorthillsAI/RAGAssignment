import requests
from bs4 import BeautifulSoup
import sys
from typing import Dict, List, Optional

class MedlinePlusScraper:
    """Class to handle scraping of MedlinePlus encyclopedia articles."""
    
    BASE_URL = "https://medlineplus.gov/ency/"
    
    def __init__(self):
        """Initialize the scraper with session for connection reuse."""
        self.session = requests.Session()
    
    def retrieve_webpage(self, url: str) -> Optional[str]:
        """
        Retrieve HTML content from a URL.
        
        Args:
            url: The URL to retrieve content from
            
        Returns:
            HTML content as string or None if retrieval failed
        """
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()  # Raise exception for 4XX/5XX responses
            return response.text
        except requests.RequestException as e:
            print(f"Error retrieving {url}: {e}")
            return None
    
    def parse_article_content(self, html: str) -> Dict[str, str]:
        """
        Extract article content from HTML.
        
        Args:
            html: HTML content to parse
            
        Returns:
            Dictionary with article sections and their content
        """
        try:
            soup = BeautifulSoup(html, "html.parser")
            
            # Extracting article title
            title_tag = soup.find("h1", class_="with-also", itemprop="name")
            article_title = title_tag.get_text(strip=True) if title_tag else "Title not found"
            
            extracted_text = {"Title": article_title}
            
            # Extract all sections dynamically
            for section in soup.find_all("div", class_="section"):
                title_div = section.find("div", class_="section-title")
                body_div = section.find("div", class_="section-body")
                
                if title_div and body_div:
                    section_title = title_div.get_text(strip=True)
                    section_content = body_div.get_text(" ", strip=True)
                    extracted_text[section_title] = section_content
            
            return extracted_text
        except Exception as e:
            print(f"Error parsing article content: {e}")
            return {"Error": f"Failed to parse content: {str(e)}"}
    
    def find_encyclopedia_articles(self, letter: str) -> List[str]:
        """
        Find all article links for a given letter in the encyclopedia.
        
        Args:
            letter: Single letter to retrieve articles for
            
        Returns:
            List of article URLs
        """
        try:
            # Validate input
            if not letter or len(letter.strip()) != 1 or not letter.strip().isalpha():
                raise ValueError("Please provide a single alphabetical character")
                
            letter = letter.strip().upper()
            url = f"{self.BASE_URL}encyclopedia_{letter}.htm"
            html = self.retrieve_webpage(url)
            
            if not html:
                return []
            
            soup = BeautifulSoup(html, "html.parser")
            article_links = []
            
            # Find all article links
            for li in soup.select("#mplus-content li"):
                if not li.get("class"):  # Ensure <li> has no class
                    a_tag = li.find("a", href=True)
                    if a_tag and a_tag["href"].startswith("article/"):
                        article_links.append(self.BASE_URL + a_tag["href"])
            
            return article_links
        except Exception as e:
            print(f"Error finding articles: {e}")
            return []
    
    def scrape_and_display_articles(self, letter: str) -> None:
        """
        Main function to scrape and display articles for a given letter.
        
        Args:
            letter: Single letter to retrieve articles for
        """
        try:
            article_links = self.find_encyclopedia_articles(letter)
            
            if not article_links:
                print(f"No articles found for letter '{letter}'.")
                return
            
            print(f"Found {len(article_links)} articles for letter '{letter}'.")
            
            for link in article_links:
                print(f"\nExtracting from: {link}")
                html = self.retrieve_webpage(link)
                
                if html:
                    extracted_text = self.parse_article_content(html)
                    for section, text in extracted_text.items():
                        print(f"\n{section}:\n{text}")
                else:
                    print(f"Could not retrieve content from {link}")
                    
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


def main():
    """Main entry point of the application."""
    try:
        scraper = MedlinePlusScraper()
        letter = input("Enter a letter to retrieve articles (A-Z): ").strip()
        scraper.scrape_and_display_articles(letter)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(0)
    except Exception as e:
        print(f"Program error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()