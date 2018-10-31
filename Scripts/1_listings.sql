#################################################################

#		DARKNETMARKETS TEAM
# 		ETL SCRIPTS: 1_LISTINGS.SQL
#		MSCA DATA ENGINEERING SPRING 2018
#		ANNA BLADEY, KUNAL SHUKLA, ANGIE TAY, WILL ZHU

#################################################################




CREATE DATABASE IF NOT EXISTS darknetmarkets; 

############################## Valhalla ##############################
# Valhalla Marketplace listings October 2016
# Format: Marketplace ID, Title, Vendor, Price, Vendor Specified Ship From Counter
# URL: https://polecat.mascherari.press/onionscan/dark-web-data-dumps

## IMPORTING DATA:

USE darknetmarkets;


DROP TABLE IF EXISTS `valhalla_listings`;

CREATE TABLE `valhalla_listings` (
	`marketplace_id` INT(16) NOT NULL,
    `title` VARCHAR(200),
    `vendor` VARCHAR(50),
    `price_eur` VARCHAR(50),
    `ship_from_country` VARCHAR(50),
    PRIMARY KEY (`marketplace_id`)
)   ENGINE=InnoDB DEFAULT CHARSET=latin1;

# CHARSET = latin1. Valhalla's title column contains non utf-8 characters. Those data will be skipped if CHARSET is set to utf-8
# Change the datapath to where you saved the raw data on your local computer
LOAD DATA LOCAL INFILE
	'/Users/annabladey/Documents/Data_Science/MScA_coursework/18S/Databases/Course_Project/20180519_ETL/data/valhalla_listings.csv'
INTO TABLE
	`valhalla_listings`
FIELDS 
	TERMINATED BY ','
    ENCLOSED BY '"'
LINES TERMINATED BY '\n';


# Add ESCAPED BY after ENCLOSED BY to skip importing some special characters

SELECT COUNT(*)
FROM valhalla_listings;

# Check that all listings are successfully imported
# Count should = 16511 listings

## DATA CLEANING:

# 1. Remove EUR from 'price_eur' 
SET SQL_SAFE_UPDATES = 0;

UPDATE 
	valhalla_listings
SET 
	price_eur = REPLACE(price_eur, 'EUR', '');

# 2. Remove all unwanted leading and/or trailing spaces

UPDATE 
	valhalla_listings
SET 
	marketplace_id = TRIM(marketplace_id),
    title = TRIM(title),
    vendor = TRIM(vendor),
    price_eur = TRIM(price_eur),
    ship_from_country = TRIM(ship_from_country);

# 3. Change the data type of the 'price_eur' column from varchar to decimal

ALTER TABLE
	valhalla_listings
MODIFY COLUMN
	price_eur DECIMAL(10,2);

## CONVER EUR TO USD

UPDATE
	valhalla_listings
SET
	price_eur = price_eur * 1.0567;
    
# RENAME PRICE_EUR TO PRICE_USD

ALTER TABLE
	valhalla_listings
CHANGE 
	price_eur price_usd DECIMAL(10,2);


############################## Hansa ##############################
# Hansa marketplace listings December 2016
# Format: Timestamp (when the listing was scrapped), Marketplace ID, Title, Vendor, Price, Marketplace Category, Ships from Country

## IMPORTING DATA:

USE darknetmarkets;

DROP TABLE IF EXISTS `hansa_listings`;

CREATE TABLE `hansa_listings` (
	`timestamp` VARCHAR(100),
    `marketplace_id` VARCHAR(16) NOT NULL,
    `title` VARCHAR (200),
    `vendor` VARCHAR(50),
    `price_usd` VARCHAR(50),
    `marketplace_category` VARCHAR(50),
    `ship_from_country` VARCHAR(50),
    PRIMARY KEY (`marketplace_id`)
)   ENGINE=InnoDB DEFAULT CHARSET=latin1;


LOAD DATA LOCAL INFILE
	'/Users/annabladey/Documents/Data_Science/MScA_coursework/18S/Databases/Course_Project/20180519_ETL/data/hansa_listings.csv'
INTO TABLE
	`hansa_listings`
FIELDS 
	TERMINATED BY ', '
    ENCLOSED BY '"'
LINES TERMINATED BY '\n';

SELECT COUNT(*)
FROM hansa_listings;

# Check that all rows were properly imported
# Count should be = 14507 out of 14773 listings have been imported. 266 rows are duplicates. Good to go! 

## DATA CLEANING:

# 1. Remove USD from 'price_usd' 
SET SQL_SAFE_UPDATES = 0;

UPDATE 
	hansa_listings
SET 
	price_usd = REPLACE(price_usd, 'USD', '');
    
UPDATE 
	hansa_listings
SET
	price_usd = REPLACE(price_usd, ',', '');

# 2. Remove all unwanted leading and/or trailing spaces

UPDATE 
	hansa_listings
SET 
	timestamp = TRIM(timestamp),
	marketplace_id = TRIM(marketplace_id),
    title = TRIM(title),
    vendor = TRIM(vendor),
    price_usd = TRIM(price_usd),
    marketplace_category = TRIM(marketplace_category),
    ship_from_country = TRIM(ship_from_country);
    
# 3. Remove data with price_usd = NULL or invalid values

DELETE FROM 
	hansa_listings
WHERE
	price_usd IS NULL OR price_usd = '' or price_usd REGEXP '[a-zA-Z]';
    
# Some rows have been shifted, REGEXP '[a-zA-Z]' is used to removed rows that contain non-numeric data
    
# 4. Change the data type of the 'price_usd' column from varchar to DECIMAL(10,2); change marketplace_id to INT(16)

ALTER TABLE
	hansa_listings
MODIFY COLUMN
	price_usd DECIMAL(10,2);
    
ALTER TABLE
	hansa_listings
MODIFY COLUMN   
    marketplace_id INT(16);

SELECT COUNT(*)
FROM hansa_listings;

# Count should = 14488, more than what we had before (14471-14488 = -17). 
# 17 listings that I removed in excel were the rows where the vendor, price, and marketplace category = ",", caused by shifted columns. It seems like the sql code above fixed that.
# Good to go!

############################## Agora ##############################

## IMPORTING DATA:
USE darknetmarkets;

DROP TABLE IF EXISTS `agora_listings`;
CREATE TABLE `agora_listings` (
    `vendor` VARCHAR(50),
    `category` VARCHAR(50),
    `item` VARCHAR(200),
    `description` VARCHAR(150),
    `price_btc` VARCHAR(50),
    `origin` VARCHAR(50),
    `destination` VARCHAR(50),
    `rating` VARCHAR(50),
    `remarks` VARCHAR(200)
)   ENGINE = InnoDB DEFAULT CHARSET = latin1;

LOAD DATA LOCAL INFILE
    '/Users/annabladey/Documents/Data_Science/MScA_coursework/18S/Databases/Course_Project/20180519_ETL/data/agora_listings.csv'
INTO TABLE
    `agora_listings`
FIELDS 
    TERMINATED BY ','
    ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


## DATA CLEANING:
# 1. Remove BTC from 'price_btc' 
SET SQL_SAFE_UPDATES = 0;
UPDATE 
    agora_listings
SET 
    price_btc = REPLACE(price_btc, 'BTC', '');
    
# 2. Remove data with price_btc = NULL or invalid values
DELETE FROM 
    agora_listings
WHERE
    price_btc IS NULL OR price_btc = '' or price_btc REGEXP '[a-zA-Z]';
    
# 3. Convert BTC to USD
UPDATE 
    agora_listings
SET 
    price_btc = price_btc*397.46;
    
# 397.46 = BTC to USD rate
 
# 4. Change column name from price_btc to price_usd and change data type from VARCHAR(50) to DECIMAL(10,2)
ALTER TABLE `agora_listings` CHANGE COLUMN `price_btc` `price_usd` DECIMAL(10,2);



############################## fact_listings Table ##############################

DROP TABLE IF EXISTS `fact_listings`;

CREATE TABLE IF NOT EXISTS `fact_listings` (
	`listingID` INT(16) NOT NULL AUTO_INCREMENT,
    `title` VARCHAR(200),
    `description` VARCHAR(150),
	`category` VARCHAR(100),
	`priceUSD` DECIMAL(10,2),
	`vendor` VARCHAR(50),
	`shipFromCountry` VARCHAR(50),
	`marketplace` VARCHAR(10) NOT NULL,
    PRIMARY KEY (`listingID`))
ENGINE = InnoDB 
DEFAULT CHARSET = latin1
AUTO_INCREMENT = 1;

# priceUSD in this table is DECIMAL(10,2) instead of VARCHAR(16). CHARSET is latin1

# 1. Insert Valhalla's Data

INSERT INTO 
	fact_listings(title, vendor, priceUSD, shipFromCountry, marketplace)
	SELECT 
		title,
		vendor,
		price_usd,
		ship_from_country,
		'Valhalla'
	FROM
		valhalla_listings
	ORDER BY
		marketplace_id;
		
# 2. Insert Hansa's Data

ALTER TABLE `fact_listings` AUTO_INCREMENT = 1;

# Reset auto_increment value otherwise it will skip some numbers. 

INSERT INTO 
	fact_listings(title, vendor, category, priceUSD, shipFromCountry, marketplace)
    SELECT 
		title,
		vendor,
        marketplace_category,
		price_usd,
		ship_from_country,
		'Hansa'
	FROM
		hansa_listings
	ORDER BY
		marketplace_id;

# 3. Insert Agora's Data
ALTER TABLE `fact_listings` AUTO_INCREMENT = 1;

# Reset auto_increment value otherwise it will skip some numbers. 
INSERT INTO 
    fact_listings(title, description, vendor, category, priceUSD, shipFromCountry, marketplace)
    SELECT 
        item,
        description,
        vendor,
        category,
        price_usd,
        origin,
        'Agora'
    FROM
        agora_listings;
        
        
# drop original tables market-specific tables       
DROP TABLE IF EXISTS valhalla_listings, hansa_listings, agora_listings;


