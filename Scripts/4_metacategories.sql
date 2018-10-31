#################################################################

#		DARKNETMARKETS TEAM
# 		ETL SCRIPTS: 4_METACATEGORIES.SQL
#		MSCA DATA ENGINEERING SPRING 2018
#		ANNA BLADEY, KUNAL SHUKLA, ANGIE TAY, WILL ZHU

#################################################################


# create category and metacategory tables


USE darknetmarkets;

## Create dim_category table
DROP TABLE IF EXISTS `dim_category`;

CREATE TABLE IF NOT EXISTS `dim_category` (
  `category_ID` INT(10) NOT NULL AUTO_INCREMENT,
  `category_name` VARCHAR(100) NOT NULL,
  `metacategory_ID` INT(10) DEFAULT NULL,
  PRIMARY KEY (`category_ID`))
ENGINE = InnoDB
AUTO_INCREMENT = 1
DEFAULT CHARACTER SET = latin1;

INSERT INTO dim_category
(category_name)
SELECT DISTINCT category
FROM fact_listings
GROUP BY category; 
   
   
## Create dim_metacategory table

DROP TABLE IF EXISTS `dim_metacategory`;

CREATE TABLE IF NOT EXISTS `dim_metacategory` (
  `metacategory_ID` INT(10) NOT NULL AUTO_INCREMENT,
  `metacategory_name` VARCHAR(100) NOT NULL,
  PRIMARY KEY (`metacategory_ID`))
ENGINE = InnoDB
AUTO_INCREMENT = 100
DEFAULT CHARACTER SET = latin1;

INSERT INTO `dim_metacategory`
(`metacategory_name`) 
VALUES
('Counterfeits'),
('Drugs'),
('Electronics'),
('Forgeries'),
('Fraud Related'),
('Hacked Accounts'),
('Info/eBooks/Guides/Tutorials'),
('Jewelry'),
('Pirated'),
('Reagents/Lab Chemicals'),
('Services'),
('Weapons'),
('Misc');

## Insert metacategory ID into dim_category table

SET 
	SQL_SAFE_UPDATES = 0;
UPDATE 
	dim_category 
INNER JOIN 
	dim_metacategory ON LEFT(dim_metacategory.metacategory_name, 5) = LEFT(dim_category.category_name, 5) 
SET 
	dim_category.metacategory_ID = dim_metacategory.metacategory_ID;

SET 
	SQL_SAFE_UPDATES = 0;
UPDATE 
	dim_category 
SET 
	metacategory_ID = '112'
WHERE 
	category_name = '';

SET 
	SQL_SAFE_UPDATES = 0;
UPDATE 
	dim_category 
SET 
	metacategory_ID = '112'
WHERE 
	category_name = '';
    

UPDATE
	dim_category
SET
	metacategory_id = '112'
WHERE
	metacategory_id IS NULL;

    
## Add contraint

ALTER TABLE `darknetmarkets`.`dim_category` 
ADD INDEX `fk_metacategory_ID_idx` (`metacategory_ID` ASC);

ALTER TABLE `darknetmarkets`.`dim_category` 
ADD CONSTRAINT `fk_metacategory_ID`
  FOREIGN KEY (`metacategory_ID`)
  REFERENCES `darknetmarkets`.`dim_metacategory` (`metacategory_ID`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION;

## Update the fact_listings/listings table. Replace category name with category ID and add foreign key   

SET 
	SQL_SAFE_UPDATES = 0;
    
    
UPDATE 
	fact_listings 
INNER JOIN 
	dim_category 
ON fact_listings.category = dim_category.category_name 
SET 
	fact_listings.category = dim_category.category_ID;
    
    
ALTER TABLE
	`fact_listings`
CHANGE COLUMN
	`category` `categoryID` INT(10);
    

ALTER TABLE `darknetmarkets`.`fact_listings` 
ADD CONSTRAINT `fk_category_ID_fct`
  FOREIGN KEY (`categoryID`)
  REFERENCES `darknetmarkets`.`dim_category` (`category_ID`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION;



