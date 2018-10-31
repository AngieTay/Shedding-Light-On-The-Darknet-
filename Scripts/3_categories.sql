#################################################################

#		DARKNETMARKETS TEAM
# 		ETL SCRIPTS: 3_CATEGORIES.SQL
#		MSCA DATA ENGINEERING SPRING 2018
#		ANNA BLADEY, KUNAL SHUKLA, ANGIE TAY, WILL ZHU

#################################################################


# CLEAN UP fact_listings.category

USE darknetmarkets;

UPDATE 
    fact_listings
SET 
    category = 'Unclassified'
WHERE 
    category = '';
    
    
UPDATE 
    fact_listings
SET 
    category = 'Drugs/Cannabis'
WHERE 
    category LIKE '%Drugs/Cannabis%';
    
    
UPDATE 
    fact_listings
SET 
    category = 'Drugs/Opioids'
WHERE 
    category LIKE '%Drugs/Opioids%';
    
    
UPDATE 
    fact_listings
SET 
    category = 'Drugs/Dissociatives'
WHERE 
    category LIKE '%Drugs/Dissociatives%';
    
    
UPDATE 
    fact_listings
SET 
    category = 'Drugs/Ecstasy'
WHERE 
    category LIKE '%Drugs/Ecstasy%';
    
    
UPDATE 
    fact_listings
SET 
    category = 'Drugs/Psychedelics'
WHERE 
    category LIKE '%Drugs/Psychedelics%';
    
    
UPDATE 
    fact_listings
SET 
    category = 'Drugs/Stimulants'
WHERE 
    category LIKE '%Drugs/Stimulants%';
    
    
UPDATE 
    fact_listings
SET 
    category = 'Drugs/Unclassified'
WHERE 
    category = 'Drugs' OR category = 'Drugs/Other';
    
    
UPDATE
    fact_listings
SET
    category = 'Unclassified'
WHERE
    category LIKE '%Godspeed%';
    
    
UPDATE
    fact_listings
SET
    category = 'Jewelry'
WHERE
    category LIKE '%Jewel%';
    
UPDATE
    fact_listings
SET
    category = 'Counterfeits/Unclassified'
WHERE
    category = 'Counterfeits';
    
    
UPDATE
    fact_listings
SET
    category = 'Services'
WHERE
    category = 'Services' OR category = 'Services/Other';
    
    
    
UPDATE
    fact_listings
SET
    category = 'Unclassified'
WHERE
    category = 'Thrills' OR category = 'Other';
    
UPDATE
    fact_listings
SET
    category = 'Unclassified'
WHERE
    category = 'Miscellaneous';
    
UPDATE
    fact_listings
SET
    category = 'Unclassified'
WHERE
    category LIKE '%1%';
    
UPDATE
    fact_listings
SET
    category = 'Forgeries/Unclassified'
WHERE
    category LIKE '%Forgeries%' OR category LIKE '%Forgeries/Other%';
    
UPDATE
    fact_listings
SET
    category = 'Drug Paraphernalia'
WHERE
    category LIKE '%Drug paraphernalia%';
    
UPDATE
    fact_listings
SET
    category = 'Unclassified'
WHERE
    category LIKE '%"%';
    
UPDATE
    fact_listings
SET
    category = 'Drugs/Tobacco'
WHERE
    category = 'Tobacco';
    
UPDATE
    fact_listings
SET
    category = 'Info/eBooks/Unclassified'
WHERE
    category LIKE '%Information/eBooks%' OR category LIKE '%Information/Guides%';
    
UPDATE
    fact_listings
SET
    category = 'Unclassified'
WHERE
    category LIKE '%BTC%';
    
UPDATE
    fact_listings
SET
    category = 'Unclassified'
WHERE
    category LIKE 'Kind Hearts';
    
UPDATE
    fact_listings
SET
    category = 'Unclassified'
WHERE
    category LIKE '%.%';
    
UPDATE
    fact_listings
SET
    category = 'Unclassified'
WHERE
    category LIKE '%Sã%';


UPDATE
	fact_listings
SET
	category = 'Unclassified'
WHERE
	category IS NULL;