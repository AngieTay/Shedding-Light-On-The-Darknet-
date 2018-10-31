#################################################################

#		DARKNETMARKETS TEAM
# 		ETL SCRIPTS: 2_COUNTRY.SQL
#		MSCA DATA ENGINEERING SPRING 2018
#		ANNA BLADEY, KUNAL SHUKLA, ANGIE TAY, WILL ZHU

#################################################################


# cleaning up fact_listings.shipFromCountry


USE darknetmarkets;

SET SQL_SAFE_UPDATES=0;

# TRIM WHITESPACE
UPDATE
	fact_listings
SET
	shipFromCountry = TRIM(shipFromCountry);

# REMOVE NULLS
UPDATE
	fact_listings
SET
	shipFromCountry = ''
WHERE
	shipFromCountry IS NULL;

# CREATE "MULTIPLE' CATEGORY
UPDATE
	fact_listings
SET
	shipFromCountry = 'multiple'
WHERE
	(shipFromCountry LIKE '% %' ) AND (shipFromCountry NOT IN('United States', 'United Kingdom', 'Europe (EU)', 'Hong Kong', 'Czech Republic'));

# ONE-BY-ONE
UPDATE
	fact_listings
SET
	shipFromCountry = 'American Samoa'
WHERE
	shipFromCountry = 'AmericanSamoa';
    

UPDATE
	fact_listings
SET
	shipFromCountry = 'Australia'
WHERE
	shipFromCountry = 'ChristmasIsland';
    
    
UPDATE
	fact_listings
SET
	shipFromCountry = ''
WHERE
	shipFromCountry IN(
    '<imgclass=\"flag-img\"src=\"/images/flag/w13/AUS.png\"',
    '<imgclass=\"flag-img\"src=\"/images/flag/w13/USA.png\"',
    'Agora',
    'bluerave',
    'Cheqdropz',
    'GeorgeWashingtonsBoner',
    'Internet',
    'La',
    'Me',
    'mypm',
    'Shipping',
    'South',
    'The',
    'theloinsof',
    'Torland',
    'TorlandNZ',
    'TorlandWorld',
    'u',
    'Undeclared;)',
    'undeclared',
    'Undelcared;)',
    'United',
    'West');


UPDATE
	fact_listings
SET
	shipFromCountry = 'Brazil'
WHERE
	shipFromCountry = 'Brasil';


UPDATE
	fact_listings
SET
	shipFromCountry = 'Cayman Islands'
WHERE
	shipFromCountry = 'CaymanIslands';
    
    
UPDATE
	fact_listings
SET
	shipFromCountry = 'Czech Republic'
WHERE
	shipFromCountry = 'Czech';
    
UPDATE
	fact_listings
SET
	shipFromCountry = 'Czech Republic'
WHERE
	shipFromCountry = 'CzechRepublic';
    
UPDATE
	fact_listings
SET
	shipFromCountry = 'Dominican Republic'
WHERE
	shipFromCountry = 'DominicanRepublic';


UPDATE
	fact_listings
SET
	shipFromCountry = 'Europe'
WHERE
	shipFromCountry = 'EU' OR shipFromCountry = 'Europe (EU)';
    
    
UPDATE
	fact_listings
SET
	shipFromCountry = 'Germany'
WHERE
	shipFromCountry = 'German' OR shipFromCountry = 'GermanyGermany';
    
    
UPDATE
	fact_listings
SET
	shipFromCountry = 'China'
WHERE
	shipFromCountry = 'Hong' or shipFromCountry = 'HongKong' or shipFromCountry = 'Hong Kong';


UPDATE
	fact_listings
SET
	shipFromCountry = 'Dominican Republic'
WHERE
	shipFromCountry = 'DominicanRepublic';
    
    
UPDATE 
    fact_listings
SET
    shipFromCountry = 'multiple'
WHERE 
    shipFromCountry IN (
    'Asia',
    'CanadaIE',
    'Canadaus',
    'CanadaUSA',
    'ChinaorEU',
    'EUNZ',
    'EuropeUSA',
    'GermanyCanada',
    'GermanyEU',
    'GermanyWORLDWIDE',
    'Latinamerica/no',
    'NorthAmerica',
    'Pacific',
    'Scandinavia',
    'UKandIreland',
    'UKEU.USA',
    'UKEU',
    'Usa&UK',
    'USACanada',
    'World',
    'WorldEU',
    'Worldwide');
    

UPDATE
	fact_listings
SET
	shipFromCountry = 'Netherlands'
WHERE
	shipFromCountry = 'NetherlandsAntilles';
    

UPDATE
	fact_listings
SET
	shipFromCountry = 'New Zealand'
WHERE
	shipFromCountry = 'NewZealand';
    
UPDATE
	fact_listings
SET
	shipFromCountry = 'Russia'
WHERE
	shipFromCountry = 'RussianFederation';
    
UPDATE
	fact_listings
SET
	shipFromCountry = 'St. Vincent and the Grenadines'
WHERE
	shipFromCountry = 'SaintVincent' or shipFromCountry = 'Saint Vincent and the Grenadines';
    
UPDATE
	fact_listings
SET
	shipFromCountry = 'South Africa'
WHERE
	shipFromCountry = 'SouthAfrica';
    
    
UPDATE
	fact_listings
SET
	shipFromCountry = 'Sri Lanka'
WHERE
	shipFromCountry = 'SriLanka';
    
UPDATE
	fact_listings
SET
	shipFromCountry = 'Thailand'
WHERE
	shipFromCountry = 'Bangkok';
    
    
UPDATE 
    fact_listings
SET
    shipFromCountry = 'United Kingdom'
WHERE 
    shipFromCountry IN (
    'UK',
    'UnitedKingdom',
    'UntiedKingdom');
    
    
     
UPDATE 
    fact_listings
SET
    shipFromCountry = 'United States'
WHERE 
    shipFromCountry IN ('TheUnitedSnakes',
    'U.S.A.',
    'US',
    'USA');



UPDATE 
    fact_listings
SET
    shipFromCountry = 'Netherlands'
WHERE 
    shipFromCountry = 'TheNetherlands';
    
    
UPDATE
	fact_listings
SET
	shipFromCountry = 'Slovak Republic'
WHERE
	shipFromCountry = 'Slovakia';
    
    
UPDATE
	fact_listings
SET
	shipFromCountry = 'Russian Federation'
WHERE
	shipFromCountry = 'Russia';