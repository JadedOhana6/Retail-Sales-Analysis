# Retail Sales Analysis using Postgres SQL. Viz on PowerBI

## Table of Contents
-	[Project Overview](#project-overview)
- [Tools Used](#tools-used)
- [Data_Source & Tables](#data-source-and-tables)
- [Data Cleaning & Preparation](#data-cleaning-and-preparation)
- [Exploratory Analysis](#exploratory-analysis)
- [Interesting codes](#interesting-codes)
- [Results & Findings](#results-and-findings)
- [Recommendations](#recommendations)
- [Limitations](#limitations)

### Project Overview
This analysis aims to provide insights regarding the sales performance of a retail store.

### Tools Used
1. Postgresql for cleaning and analysis
2. PowerBI for visualization

### Data Source and Tables
[Kaggle - Dirty for Data Cleaning](https://www.kaggle.com/datasets/ahmedmohamed2003/retail-store-sales-dirty-for-data-cleaning/data)
- retailsales
- EHE category (scraped from Kaggle using [Instant Data Scraper](https://chromewebstore.google.com/detail/instant-data-scraper/ofaokhiedipichpaobibbnahnkdoiiah))
- FUR category (scraped from Kaggle page using [Instant Data Scraper](https://chromewebstore.google.com/detail/instant-data-scraper/ofaokhiedipichpaobibbnahnkdoiiah))
- 6 other category tables are not available, but they all share the same pattern as the existing 2. 

## Data Cleaning and Preparation
The following tasks were performed:
1. Used GEMINI's help to write query for the category tables
2. Checked for duplicates, handle missing data and inconsistent formating

## Exploratory Analysis
1. Sales trend over time
2. Which products should we order more of or less of?
- Analysis method: Priority products for restocking by:
  - product demand = SUM (quantityOrdered)/quantityInStock
  - product performance =SUM(quantityOrdered×priceEach)
3. How much can we spend on acquiring customers?
  - Analysis method: Customer Lifetime Value (LTV), which represents the average amount of money a customer generates

### Results and Findings

## Interesting codes

## Recommendations

## Limitations
1. Unable to determine the effectiveness of discounts. The Discount Applied column in the retailsales table is missing data, and the table does not record any price changes upon discount. 
2. No item name, but this will just affect the visuals. We are still able to use item code for analysis instead.
