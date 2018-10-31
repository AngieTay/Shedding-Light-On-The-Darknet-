#################################################################

#		DARKNETMARKETS TEAM
# 		ETL SCRIPTS: 5_COUNTRYSTATS.SQL
#		MSCA DATA ENGINEERING SPRING 2018
#		ANNA BLADEY, KUNAL SHUKLA, ANGIE TAY, WILL ZHU

#################################################################


# create dim_GDP and dim_population tables
USE darknetmarkets;

DROP TABLE IF EXISTS dim_GDP;
CREATE TABLE IF NOT EXISTS `dim_GDP` (
    `country` VARCHAR(200),
    `2014` DECIMAL(20,2),
    `2015` DECIMAL(20,2),
    `2016` DECIMAL(20,2))
ENGINE = InnoDB 
DEFAULT CHARSET = latin1
AUTO_INCREMENT = 1;

LOAD DATA LOCAL INFILE
	'/Users/annabladey/Documents/Data_Science/MScA_coursework/18S/Databases/Course_Project/20180519_ETL/data/GDP.csv'
INTO TABLE
	dim_GDP
FIELDS
	TERMINATED BY','
	ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


ALTER TABLE dim_GDP 
ADD country_ID INT PRIMARY KEY AUTO_INCREMENT;



DROP TABLE IF EXISTS dim_population;
CREATE TABLE IF NOT EXISTS `dim_population` (
    `country` VARCHAR(200),
    `2014` DECIMAL(20,2),
    `2015` DECIMAL(20,2),
    `2016` DECIMAL(20,2))
ENGINE = InnoDB 
DEFAULT CHARSET = latin1
AUTO_INCREMENT = 1;

LOAD DATA LOCAL INFILE
	'/Users/annabladey/Documents/Data_Science/MScA_coursework/18S/Databases/Course_Project/20180519_ETL/data/population.csv'
INTO TABLE
	dim_population
FIELDS
	TERMINATED BY','
	ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


ALTER TABLE dim_population 
ADD country_ID INT PRIMARY KEY AUTO_INCREMENT;


# combine GDP and population tables into dim_countrystats

DROP TABLE IF EXISTS `dim_countrystats`;

CREATE TABLE IF NOT EXISTS `dim_countrystats` (
	`country_ID` INT,
    `country` VARCHAR(200),
    `GDP_2014` DECIMAL(20,2) DEFAULT NULL,
    `GDP_2015` DECIMAL(20,2) DEFAULT NULL,
    `GDP_2016` DECIMAL(20,2) DEFAULT NULL,
	`population_2014` DECIMAL(20,2) DEFAULT NULL,
    `population_2015` DECIMAL(20,2) DEFAULT NULL,
    `population_2016` DECIMAL(20,2) DEFAULT NULL)
ENGINE = InnoDB 
DEFAULT CHARSET = latin1
AUTO_INCREMENT = 1;



INSERT INTO dim_countrystats (
	country_ID,
    country,
    GDP_2014,
    GDP_2015,
    GDP_2016,
	population_2014,
    population_2015,
    population_2016)
(SELECT 
	`dim_GDP`.`country_ID`,
    `dim_GDP`.`country`,
    `dim_GDP`.`2014`,
    `dim_GDP`.`2015`,
    `dim_GDP`.`2016`,
    `dim_population`.`2014`,
	`dim_population`.`2015`,
   `dim_population`.`2016`
FROM
	dim_GDP INNER JOIN dim_population ON dim_GDP.country_ID = dim_population.country_ID);

ALTER TABLE dim_countrystats
  ADD PRIMARY KEY (country_ID); 

ALTER TABLE `darknetmarkets`.`dim_countrystats` 
CHANGE COLUMN `country_ID` `country_ID` INT(11) NOT NULL AUTO_INCREMENT ;

    
DROP TABLES dim_GDP, dim_population;    
    

# insert non-country "countries" we hand-defined

INSERT INTO dim_countrystats(
    country)
VALUES
	('Europe');
    
INSERT INTO dim_countrystats(
    country)
VALUES
	('multiple');

   
# delete things that aren't countries

DELETE FROM
    dim_countrystats
WHERE
    country = 'Central African Republic' OR country = 'Central Europe and the Baltics' OR country = 'Euro area' OR country LIKE 'East Asia%'
    OR country LIKE 'Early%' OR country LIKE 'Euro%' OR country LIKE 'Fragile%' OR country LIKE 'Heavily%' OR country LIKE 'High%'
    OR country LIKE 'IBR%' OR country LIKE 'IDA%' OR country LIKE 'Korea, Dem.%' OR country LIKE 'Late%' OR country LIKE 'Latin%'
    OR country LIKE 'Least%' OR country LIKE 'Low%' OR country LIKE 'Middle%' OR country LIKE 'OECD' OR country = 'Not classified' 
    OR country LIKE 'Other%' OR country LIKE '%demographic%' OR country LIKE 'Small%' OR country LIKE 'South Asia%' OR country LIKE 'Sub-%'
    OR country LIKE 'Upper%' OR country = 'World';
    

# rename fact_listings.shipFromCountry to fact_listings.shipFromCountryID

ALTER TABLE
	`fact_listings`
CHANGE COLUMN
	`shipFromCountry` `shipFromCountryID` VARCHAR(50);
    
    
# insert country_ID into fact_listings table

        
UPDATE 
	fact_listings
LEFT JOIN dim_countrystats
ON
	fact_listings.shipFromCountryID = dim_countrystats.country
SET
	fact_listings.shipFromCountryID = dim_countrystats.country_ID;
 
 
ALTER TABLE 
	fact_listings
MODIFY COLUMN 
	shipFromCountryID INT(10);

ALTER TABLE `darknetmarkets`.`fact_listings` 
ADD CONSTRAINT `fk_shipFromCountry_ID_fct`
  FOREIGN KEY (`shipFromCountryID`)
  REFERENCES `darknetmarkets`.`dim_countrystats` (`country_ID`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION;




