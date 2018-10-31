————————————————————————————
SCRIPTS
————————————————————————————

1_listings.sql

You will have to modify paths.

Imports Valhalla, Hansa, and Agora data from CSV into MySQL
performs basic cleanup and standardization
merges all item listings into fact_listings

2_country.sql

Cleans up listings.shipFromCountry


3_categories.sql

Cleans up listings.categories


4_metacategory.sql

Creates dim_metacategory


5_countrystats.sql

You will have to modify file paths.

Imports GDP and population data, creates dim_countrystats table
