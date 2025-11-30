This directory represents the data lake or data warehouse where
the data is uploaded. 

I have outputted the data to the `src` directory when running it locally as it is raw
data from the Github API. Ideally in a production environment it would be in a file
format such as parquet as this has benefits such as immutability and compression and if
in databricks then can use delta tables as well.

I have stored the transformed data under `agg/pull_requests` to represent the table
of data. 



`src`
- Represents the raw data extracted directly from the source. 
- This is often referred to as the "bronze" layer in medallion architecture
- This layer isn't typically used by business stakeholders, but has use cases such as
audit purposes for referencing historical data, analytical deep dives for data analysts
and data scientists

`agg`
- Represents aggregated and cleaned data one level up from `src`
- Often referred to as the "silver" layer in medallion architecture
- The data is a cleaned and aggregated version of `src` layer
- This layer is granular and can be used such as to feed into a data science model
- Usually this layer has deduplication and cleaning applied to it

`biz`
- Represents the business layer and is refined and aggregated `src` and `agg` data
- Often referred to as the "gold" layer in medallion architecture
- This layer is full cleaned and aggregated for business purposes only, such as each table
directly correlating to a business dashboard
- This is the most refined, aggregated and cleaned layer out of the three
