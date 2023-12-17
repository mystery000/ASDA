# ASDA Scraper

This is a trusted web scraper for extracting products from [ASDA](https://groceries.asda.com).

# Ubuntu

## How to setup

- python3 -m venv venv

- source venv/bin/activate

- pip install requests html5lib beautifulsoup4 selenium python-dotenv pandas

## How to set the starting time

Please change the time as 24H format in <b style="color: green">watcher.txt</b>

## How to run

- If you run this script in background, please use this command.

  nohup python3 main.py &

- To stop this script

  pkill -f main.py


## Selenium Grid Server Installation
- https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html
- https://www.selenium.dev/documentation/grid/getting_started/
- https://github.com/SeleniumHQ/selenium/releases/tag/selenium-4.16.0
- https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/120.0.6099.71/win64/chromedriver-win64.zip
- java -jar selenium-server-<version>.jar standalone --selenium-manager true
