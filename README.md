# Description

This is a trusted web scraper for extracting products from [ASDA](https://groceries.asda.com) supermarket.

# Usage

This scraper only supports Ubuntu.

## Prerequisites

- [Python](https://phoenixnap.com/kb/how-to-install-python-3-ubuntu) installed

- [Git](https://www.digitalocean.com/community/tutorials/how-to-install-git-on-ubuntu-20-04) installed

- Selenium Grid Server installed on virtual private server with Windows

  - [Java 17](https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html) or higher installed

  - [Chrome Driver](https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/120.0.6099.71/win64/chromedriver-win64.zip) installed and on the [PATH](https://www.selenium.dev/documentation/webdriver/troubleshooting/errors/driver_location/#use-the-path-environment-variable)<br /> 

  - Download the Selenium Server jar file from the [latest release](https://github.com/SeleniumHQ/selenium/releases/latest)

  - Start the Selenium Grid

    - `java -jar selenium-server-<version>.jar hup --port 9515`

    - `java -jar selenium-server-<version>.jar node`   

  - Configure `.env` with your Selenium server IP address and port

## Get the code

`git clone https://github.com/mystery000/ASDA.git` <br />

## Installation

- `sudo apt-get install python3.9-venv`

- `python3 -m venv venv`

- `source venv/bin/activate`

- `pip install requests html5lib beautifulsoup4 selenium python-dotenv pandas`

## Configuration

This scraper runs automatically at the time specified in `wathcer.txt`.<br />
Time format: `24H`

## How to run

- If you run this script in background, please use this command.

  `nohup python3 main.py &`

- To stop this script

  `pkill -f main.py`

- To look at the logs

  `cat nohup.out`
