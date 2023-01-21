# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#                                                               #
# Each row represents an individual within a set a zip codes.   #
#.Within each row is a summation and aggregate of totals        #
# they contributed to election cycles for the 2016-2018 cycle   #
# OR 2018-2020 election cycle. This table includes.             #
# individuals who contributed to EITHER cycles                  #
#                                                               #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# List of zip codes which intersect with NY 12 Congressional district
DECLARE zip_code_list ARRAY < STRING >;

SET
  zip_code_list = (
    SELECT
      ARRAY_AGG(zip_codes.zip_code)
    FROM
      # NY 12 Geo Boundaries
      (
        SELECT
          geo_id AS geo_id,
          district_geom AS district_geom
        FROM
          `bigquery-public-data.geo_us_boundaries.congress_district_116`
        WHERE
          state_fips_code = "36"
          AND district_fips_code = "12"
      ) AS congress_district_116_ny_12
      JOIN (
        SELECT
          zip_code,
          zip_code_geom AS geom
        FROM
          `bigquery-public-data`.geo_us_boundaries.zip_codes
      ) AS zip_codes ON ST_INTERSECTS(
        congress_district_116_ny_12.district_geom,
        zip_codes.geom
      )
  );

# Corrected indiv20 table
WITH indiv20 AS (
  SELECT
    *
  FROM
    (
      SELECT
        # Extract real committee id from memo_text instead of parent company (actblue)
        REGEXP_EXTRACT(memo_text, "(C[0-9]+)") AS cmte_id,
        amndt_ind,
        rpt_tp,
        transaction_pgi,
        image_num,
        transaction_tp,
        entity_tp,
        name,
        city,
        state,
        zip_code,
        employer,
        occupation,
        transaction_dt,
        transaction_amt,
        other_id,
        tran_id,
        file_num,
        memo_cd,
        memo_text,
        sub_id
      FROM
        `bigquery-public-data.fec.indiv20`
      WHERE
        cmte_id = "C00401224" #actblue committee
        #add everything that is a direct contribution
      UNION
      ALL
      SELECT
        *
      FROM
        `bigquery-public-data.fec.indiv20`
      WHERE
        cmte_id <> "C00401224" #i.e. anything that isn't actblue
    )
),
# Corrected indiv18 table
indiv18 AS (
  SELECT
    *
  FROM
    (
      SELECT
        # Extract real committee id from memo_text instead of parent company (actblue)
        REGEXP_EXTRACT(memo_text, "(C[0-9]+)") AS cmte_id,
        amndt_ind,
        rpt_tp,
        transaction_pgi,
        image_num,
        transaction_tp,
        entity_tp,
        name,
        city,
        state,
        zip_code,
        employer,
        occupation,
        transaction_dt,
        transaction_amt,
        other_id,
        tran_id,
        file_num,
        memo_cd,
        memo_text,
        sub_id
      FROM
        `bigquery-public-data.fec.indiv18`
      WHERE
        cmte_id = "C00401224" #actblue committee
        #add everything that is a direct contribution
      UNION
      ALL
      SELECT
        *
      FROM
        `bigquery-public-data.fec.indiv18`
      WHERE
        cmte_id <> "C00401224" #i.e. anything that isn't actblue
    )
),
# ny_12_2020_donations Collect all NY-12 donators donations with committee information
ny_12_2020_donations AS (
  SELECT
    cm20.cmte_id AS cmte_id_2020,
    # 2020 Committee ID
    cm20.cmte_nm AS cmte_nm_2020,
    # 2020 Comittee Name
    indiv20.name AS name_2020,
    # 2020 Individuals name "LAST, FIRST"
    indiv20.transaction_amt AS transaction_amt_2020,
    # 2020 Transaction Amount
    SUBSTR(indiv20.zip_code, 1, 5) AS zip_code_2020,
    # only first 5 digits of zip code, ignore the extra
    indiv20.employer AS employer_2020,
    # 2020 Individuals employer
    indiv20.occupation AS occupation_2020 # 2020 Individuals occupation
  FROM
    indiv20
    JOIN `bigquery-public-data.fec.cm20` cm20 ON cm20.cmte_id = indiv20.cmte_id
  WHERE
    1 = 1
    AND LEFT(indiv20.zip_code, 5) IN (
      SELECT
        zip_code
      FROM
        UNNEST(zip_code_list) zip_code
    )
),
# ny_12_2020_aggregate Bundle committee donations by an individual
ny_12_2020_aggregate AS (
  SELECT
    LOWER(
      CONCAT(
        # find unique humans by concating zipcode+firstname+lastname
        zip_code_2020,
        "+",
        REGEXP_EXTRACT(name_2020, ', ([^ ]*)'),
        "+",
        # firstname
        REGEXP_EXTRACT(name_2020, '[^ ,]*') # lastname
      )
    ) AS concat_name_2020,
    name_2020,
    # inidividuals name
    transaction_amt_2020 AS amount_2020,
    # 2020 transaction amount
    cmte_nm_2020,
    # 2020 committee name
    occupation_2020,
    # 2020 Individuals occupation
    employer_2020,
    # 2020 Individuals employer
  FROM
    ny_12_2020_donations
  WHERE
    1 = 1
  ORDER BY
    name_2020 ASC
),
# ny_12_2020_top_k Get top k donations by an individual
ny_12_2020_top_k AS (
  SELECT
    concat_name_2020,
    cmte_nm_2020,
    amount_2020,
    ROW_NUMBER() OVER (
      PARTITION BY concat_name_2020
      ORDER BY
        amount_2020 DESC
    ) AS top_k
  FROM
    ny_12_2020_aggregate #LIMIT 10000 # UNCOMMENT TO MAKE FASTER
),
# ny_12_2020_top_5 aggregate top three donations for each committee by an individual
ny_12_2020_top_5 AS (
  SELECT
    ny_12_2020_top_k.concat_name_2020,
    STRING_AGG(
      CONCAT(
        "$",
        ny_12_2020_top_k.amount_2020,
        " ",
        ny_12_2020_top_k.cmte_nm_2020
      )
    ) AS list_2020,
    # Aggregate 2020 of committee donations
  FROM
    ny_12_2020_top_k
  WHERE
    ny_12_2020_top_k.top_k <= 5
  GROUP BY
    ny_12_2020_top_k.concat_name_2020 #LIMIT 10000 # UNCOMMENT TO MAKE FASTER
),
# ny_12_2020_individual aggregate every individual's 2020 donations
ny_12_2020_individual AS (
  SELECT
    ny_12_2020_aggregate.concat_name_2020,
    SUM(ny_12_2020_aggregate.amount_2020) AS total_2020,
    # Sum all 2020 donations
    MAX(ny_12_2020_aggregate.amount_2020) AS largest_2020,
    # Largest 2020 donations
    COUNT(ny_12_2020_aggregate.amount_2020) AS number_of_donations_2020,
    # Largest 2020 donations
    ny_12_2020_top_5.list_2020,
    # Aggregate 2020 of committee donations
    STRING_AGG(
      DISTINCT CONCAT(
        " ",
        ny_12_2020_aggregate.occupation_2020,
        "/",
        ny_12_2020_aggregate.employer_2020
      )
    ) AS employment_2020,
    # Aggregate 2020 Position/Employer
  FROM
    ny_12_2020_aggregate
    JOIN ny_12_2020_top_5 ON ny_12_2020_top_5.concat_name_2020 = ny_12_2020_aggregate.concat_name_2020
  GROUP BY
    concat_name_2020,
    ny_12_2020_top_5.list_2020
),
# ny_12_2018_donations Collect all NY-12 donators donations with committee information
ny_12_2018_donations AS (
  SELECT
    cm18.cmte_id AS cmte_id_2018,
    # 2018 Committee ID
    cm18.cmte_nm AS cmte_nm_2018,
    # 2018 Comittee Name
    indiv18.name AS name_2018,
    # 2018 Individuals name "LAST, FIRST"
    indiv18.transaction_amt AS transaction_amt_2018,
    # 2018 Transaction Amount
    SUBSTR(indiv18.zip_code, 1, 5) AS zip_code_2018,
    # only first 5 digits of zip code, ignore the extra
    indiv18.employer AS employer_2018,
    # 2018 Individuals employer
    indiv18.occupation AS occupation_2018 # 2018 Individuals occupation
  FROM
    indiv18
    JOIN `bigquery-public-data.fec.cm18` cm18 ON cm18.cmte_id = indiv18.cmte_id
  WHERE
    1 = 1
    AND LEFT(indiv18.zip_code, 5) IN (
      SELECT
        zip_code
      FROM
        UNNEST(zip_code_list) zip_code
    )
),
# ny_12_2018_aggregate Get donation total, largest donation, number of donation by an individual
# for each committee they donated to
ny_12_2018_aggregate AS (
  SELECT
    LOWER(
      CONCAT(
        # find unique humans by concating zipcode+firstname+lastname
        zip_code_2018,
        "+",
        REGEXP_EXTRACT(name_2018, ', ([^ ]*)'),
        "+",
        # firstname
        REGEXP_EXTRACT(name_2018, '[^ ,]*') # lastname
      )
    ) AS concat_name_2018,
    name_2018,
    # inidividuals name
    transaction_amt_2018 AS amount_2018,
    # 2018 transaction amount
    cmte_nm_2018,
    # 2018 committee name
    occupation_2018,
    # 2018 Individuals occupation
    employer_2018,
    # 2018 Individuals employer
  FROM
    ny_12_2018_donations
  WHERE
    1 = 1
  ORDER BY
    name_2018 ASC
),
# ny_12_2018_top_k Get top k donations by an individual
ny_12_2018_top_k AS (
  SELECT
    concat_name_2018,
    cmte_nm_2018,
    amount_2018,
    ROW_NUMBER() OVER (
      PARTITION BY concat_name_2018
      ORDER BY
        amount_2018 DESC
    ) AS top_k
  FROM
    ny_12_2018_aggregate #LIMIT 10000 # UNCOMMENT TO MAKE FASTER
),
# ny_12_2018_top_5 aggregate top three donations for each committee by an individual
ny_12_2018_top_5 AS (
  SELECT
    ny_12_2018_top_k.concat_name_2018,
    STRING_AGG(
      CONCAT(
        "$",
        ny_12_2018_top_k.amount_2018,
        " ",
        ny_12_2018_top_k.cmte_nm_2018
      )
    ) AS list_2018,
    # Aggregate 2018 of committee donations
  FROM
    ny_12_2018_top_k
  WHERE
    ny_12_2018_top_k.top_k <= 5
  GROUP BY
    ny_12_2018_top_k.concat_name_2018 #LIMIT 10000 # UNCOMMENT TO MAKE FASTER
),
# ny_12_2018_individual aggregate every individual's 2018 donations
ny_12_2018_individual AS (
  SELECT
    ny_12_2018_aggregate.concat_name_2018,
    SUM(ny_12_2018_aggregate.amount_2018) AS total_2018,
    # Sum all 2018 donations
    MAX(ny_12_2018_aggregate.amount_2018) AS largest_2018,
    # Largest 2018 donations
    COUNT(ny_12_2018_aggregate.amount_2018) AS number_of_donations_2018,
    # Largest 2018 donations
    ny_12_2018_top_5.list_2018,
    # Aggregate 2018 of committee donations
    STRING_AGG(
      CONCAT(
        " ",
        ny_12_2018_aggregate.occupation_2018,
        "/",
        ny_12_2018_aggregate.employer_2018
      )
    ) AS employment_2018,
    # Aggregate 2018 Position/Employer
  FROM
    ny_12_2018_aggregate
    JOIN ny_12_2018_top_5 ON ny_12_2018_top_5.concat_name_2018 = ny_12_2018_aggregate.concat_name_2018
  GROUP BY
    concat_name_2018,
    ny_12_2018_top_5.list_2018
),
# list of nys voters concat'd by zip+firstname+lastname and their voting history
nysvoters_aggregate AS (
  SELECT
    LOWER(CONCAT(rzip5, "+", firstname, "+", lastname)) AS nysvoters_concat_name,
    MAX(enrollment) AS enrollment,
    MAX(otherparty) AS otherparty,
    MAX(countycode) AS countycode,
    MAX(ed) AS ed,
    MAX(ld) AS ld,
    MAX(towncity) AS towncity,
    MAX(ward) AS ward,
    MAX(cd) AS cd,
    MAX(sd) AS sd,
    MAX(ad) AS ad,
    MAX(lastvoteddate) AS lastvoteddate,
    MAX(prevyearvoted) AS prevyearvoted,
    MAX(prevcounty) AS prevcounty,
    MAX(prevaddress) AS prevaddress,
    MAX(prevname) AS prevname,
    MAX(countyvrnumber) AS countyvrnumber,
    MAX(regdate) AS regdate,
    MAX(vrsource) AS vrsource,
    MAX(idrequired) AS idrequired,
    MAX(idmet) AS idmet,
    MAX(status) AS status,
    MAX(reasoncode) AS reasoncode,
    MAX(inact_date) AS inact_date,
    MAX(purge_date) AS purge_date,
    MAX(sboeid) AS sboeid,
    STRING_AGG(DISTINCT voterhistory) AS voterhistory,
  FROM
    `stately-math-330421.ranaforcongress.nysvoters`
  GROUP BY
    nysvoters_concat_name
),
# list of rana donators concat'd by zip+firstname+lastname and their total contribution to ranaforcongress
ranadonations AS (
  SELECT
    LOWER(
      CONCAT(
        Donor_ZIP,
        "+",
        Donor_First_Name,
        "+",
        Donor_Last_Name
      )
    ) AS ranadonations_concat_name,
    SUM(Amount) AS rana_amount
  FROM
    `stately-math-330421.ranaforcongress.ranadonations`
  GROUP BY
    ranadonations_concat_name
),
# List of
ranalist_aggregate AS (
  SELECT
    LOWER(CONCAT(Zip, "+", FirstName, "+", LastName)) AS ranalist_concat_name,
    MAX(VANID) AS VANID,
    MAX(PreferredEmail) AS PreferredEmail,
    MAX(PreferredPhone) AS PreferredPhone,
    MAX(Address) AS Address,
    MAX(City) AS City,
    MAX(State) AS State,
    MAX(Zip) AS Zip,
    MAX(Zip4) AS Zip4,
    MAX(CountryCode) AS CountryCode,
    MAX(VoterVANID) AS VoterVANID,
    MAX(Support_Rana) AS Support_Rana,
    MAX(Volunteer_Ask) AS Volunteer_Ask,
    MAX(Donor) AS Donor,
    MAX(MonthlyDonor) AS MonthlyDonor,
    MAX(CountyName) AS CountyName,
    MAX(PrecinctName) AS PrecinctName,
    MAX(Notes) AS Notes,
  FROM
    `stately-math-330421.ranaforcongress.ranalist`
  GROUP BY
    ranalist_concat_name
) # Combine aggregate tables on 2020 OR 2018 committee donations by a single individual
# unique by individual's concat'd zip+firstname+lastname
SELECT
  ny_12_2020_individual.concat_name_2020,
  ny_12_2018_individual.total_2018,
  # Sum all 2018 donations
  ny_12_2020_individual.total_2020,
  # Sum all 2020 donations
  ny_12_2018_individual.largest_2018,
  # Largest 2018 donations
  ny_12_2020_individual.largest_2020,
  # Largest 2020 donations
  ny_12_2018_individual.number_of_donations_2018,
  # Number 2018 donations
  ny_12_2020_individual.number_of_donations_2020,
  # Number 2020 donations
  ny_12_2018_individual.list_2018,
  # Aggregate 2018 of committee donations
  ny_12_2020_individual.list_2020,
  # Aggregate 2020 of committee donations
  ny_12_2018_individual.employment_2018,
  # Aggregate 2018 of employment
  ny_12_2020_individual.employment_2020,
  # Aggregate 2020 of employment
  nysvoters_aggregate.*,
  ranalist_aggregate.*,
  ranadonations.rana_amount AS total_to_rana # Total donations to RanaForCongress
FROM
  ny_12_2020_individual
  LEFT JOIN ny_12_2018_individual ON ny_12_2018_individual.concat_name_2018 = ny_12_2020_individual.concat_name_2020
  LEFT JOIN ranadonations ON ranadonations_concat_name = ny_12_2020_individual.concat_name_2020 # Correlate individual donator's rana contributions, if exists, null if not
  LEFT JOIN nysvoters_aggregate ON nysvoters_aggregate.nysvoters_concat_name = ny_12_2020_individual.concat_name_2020 # Correlate individual donator's voting history, if exists, null if not
  LEFT JOIN ranalist_aggregate ON ranalist_aggregate.ranalist_concat_name = ny_12_2020_individual.concat_name_2020 # Correlate individual donator's voting history, if exists, null if not
ORDER BY
  total_to_rana DESC