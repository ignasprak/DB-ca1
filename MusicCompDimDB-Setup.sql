-- Drop and create the schema

--Change the schema name to be your student number
DROP SCHEMA IF EXISTS MusicCompDimDB_c20424992 CASCADE;

CREATE SCHEMA MusicCompDimDB_c20424992;

SET SEARCH_PATH TO MusicCompDimDB;

-- Create Dimension Tables

-- Edition Dimension
CREATE TABLE MusicCompDimDB_c20424992.EditionDimension (
    EditionYear INT UNIQUE PRIMARY KEY,
    PresenterName VARCHAR(100)
);

-- Participant Dimension
CREATE TABLE MusicCompDimDB_c20424992.ParticipantDimension (
    ParticipantName VARCHAR(100) PRIMARY KEY,
    CountyName VARCHAR(100)
);

-- Viewer Agegroup Dimension
CREATE TABLE MusicCompDimDB_c20424992.AgeDimension (
    AGE_GROUPID INTEGER NOT NULL,
    AGE_GROUP_DESC VARCHAR(50) DEFAULT NULL,
    PRIMARY KEY (AGE_GROUPID)
);
-- Viewer Category Dimension
CREATE TABLE MusicCompDimDB_c20424992.CategoryDimension (
    CatID INT PRIMARY KEY,
    CatName VARCHAR(50) DEFAULT NULL CHECK (
        CATNAME IN ('Jury', 'Audience')
    )
);

-- Create Fact Table (Votes by Vote Mode from Viewers in Each Category, AgeGroup in Each County)
CREATE TABLE MusicCompDimDB_c20424992.VotesFact (
    EditionYear INT,
    ParticipantName VARCHAR(100),
    VoteMode VARCHAR(10),
    ViewerCat INT,
    ViewerAgeGrp INT,
    TotalVotes INT, -- Aggregated count of votes
    PRIMARY KEY (
        EditionYear,
        ParticipantName,
        ViewerCat,
        ViewerAgeGrp,
        VoteMode
    ),
    FOREIGN KEY (EditionYear) REFERENCES MusicCompDimDB_c20424992.EditionDimension (EditionYear),
    FOREIGN KEY (ParticipantName) REFERENCES MusicCompDimDB_c20424992.ParticipantDimension (ParticipantName),
    FOREIGN KEY (ViewerCat) REFERENCES MusicCompDimDB_c20424992.CategoryDimension (CatID),
    FOREIGN KEY (ViewerAgeGrp) REFERENCES MusicCompDimDB_c20424992.AgeDimension (AGE_GROUPID)
);

-- Populate Edition Dimension
INSERT INTO
    MusicCompDimDB_c20424992.EditionDimension (EditionYear, PresenterName)
SELECT EDYEAR, EDPRESENTER
FROM MusicCompDB.Edition;

-- Populate Participant Dimension
INSERT INTO
    MusicCompDimDB_c20424992.ParticipantDimension (ParticipantName, CountyName)
SELECT p.PARTNAME, c.COUNTYNAME
FROM MusicCompDB.PARTICIPANTS p
    LEFT JOIN MusicCompDB.COUNTY c ON p.COUNTYID = c.COUNTYID;

-- Populate Age Group Dimension
INSERT INTO
    MusicCompDimDB_c20424992.AgeDimension (AGE_GROUPID, AGE_GROUP_DESC)
SELECT AGE_GROUPID, AGE_GROUP_DESC
FROM MusicCompDB.AGEGROUP;

-- Populate the Category Dimension
INSERT INTO
    MusicCompDimDB_c20424992.CategoryDimension (CatID, CatName)
SELECT CATID, CATNAME
FROM MusicCompDB.VIEWERCATEGORY;

-- C20424992 - Q1

-- C20424992 - Create another Fact Table, that pre-aggregates data by year, participant, county, vote mode, age group and category
CREATE TABLE MusicCompDimDB_c20424992.VotesSummaryFact (
    EditionYear INT,
    ParticipantName VARCHAR(100),
    CountyName VARCHAR(100),
    VoteMode VARCHAR(10),
    ViewerAgeGrp INT,
    TotalVotes INT,
    PRIMARY KEY (
        EditionYear,
        ParticipantName,
        CountyName,
        VoteMode,
        ViewerAgeGrp
    )
);
-- This table will be populated with an ETL job that will summarise data from VotesFact

-- C20424992 - Create a County Dimension table to normalise dimensions
CREATE TABLE MusicCompDimDB_c20424992.CountyDimension (
    CountyID SERIAL PRIMARY KEY, -- alternate key for county
    CountyName VARCHAR(100) UNIQUE NOT NULL
);

-- C20424992 - SQL for altering tables in order to utilise CountyDimension table

-- C20424992 - Update ParticipantDimension to reference CountyDimension
ALTER TABLE MusicCompDimDB_c20424992.ParticipantDimension
DROP COLUMN CountyName;

-- C20424992 - Update VotesFact to include a CountyID column to reference CountyDimension
ALTER TABLE MusicCompDimDB_c20424992.ParticipantDimension
ADD COLUMN CountyID INT,
ADD CONSTRAINT fk_participant_county FOREIGN KEY (CountyID) REFERENCES MusicCompDimDB_c20424992.CountyDimension (CountyID);

ALTER TABLE MusicCompDimDB_c20424992.VotesFact
ADD COLUMN CountyID INT,
ADD CONSTRAINT fk_votes_county FOREIGN KEY (CountyID) REFERENCES MusicCompDimDB_c20424992.CountyDimension (CountyID);

-- C20424992 - Populate CountyDimension with unique county names
INSERT INTO
    MusicCompDimDB_c20424992.CountyDimension (CountyName)
SELECT DISTINCT
    c.COUNTYNAME
FROM MusicCompDB.PARTICIPANTS p
    LEFT JOIN MusicCompDB.COUNTY c ON p.COUNTYID = c.COUNTYID;

-- C20424992 - Temporarily add CountyName to ParticipantDimension
ALTER TABLE MusicCompDimDB_c20424992.ParticipantDimension
ADD COLUMN CountyName VARCHAR(100);

-- C20424992 - Populate CountyName using a join with the original source tables
UPDATE MusicCompDimDB_c20424992.ParticipantDimension pd
SET
    CountyName = c.COUNTYNAME
FROM MusicCompDB.PARTICIPANTS p
    LEFT JOIN MusicCompDB.COUNTY c ON p.COUNTYID = c.COUNTYID
WHERE
    pd.ParticipantName = p.PARTNAME;

-- C20424992 - Assign correct CountyID to each participant by joining with CountyDimension
UPDATE MusicCompDimDB_c20424992.ParticipantDimension pd
SET
    CountyID = cd.CountyID
FROM MusicCompDimDB_c20424992.CountyDimension cd
WHERE
    pd.CountyName = cd.CountyName;

-- C20424992 - Drop the temporary CountyName column
ALTER TABLE MusicCompDimDB_c20424992.ParticipantDimension
DROP COLUMN CountyName;

-- Make sure County ID's match
UPDATE MusicCompDimDB_c20424992.ParticipantDimension pd
SET
    countyid = p.countyid
FROM musiccompdb.participants p
WHERE
    pd.participantname = p.partname
    AND pd.countyid <> p.countyid;

-- C20424992 - Assign the correct CountyID to each vote record in VotesFact
UPDATE MusicCompDimDB_c20424992.VotesFact vf
SET
    CountyID = pd.CountyID
FROM MusicCompDimDB_c20424992.ParticipantDimension pd
WHERE
    vf.ParticipantName = pd.ParticipantName;

-- C20424992 - Populate the VotesFact table
INSERT INTO
    MusicCompDimDB_c20424992.VotesFact (
        EditionYear,
        ParticipantName,
        VoteMode,
        ViewerCat,
        ViewerAgeGrp,
        TotalVotes,
        CountyID
    )
SELECT
    v.EDITION_YEAR,
    v.PARTNAME,
    v.VOTEMODE,
    vw.CATEGORY AS ViewerCat,
    vw.AGE_GROUP AS ViewerAgeGrp,
    COUNT(v.VOTE) AS TotalVotes,
    pd.CountyID
FROM MusicCompDB.VOTES v
    JOIN MusicCompDB.VIEWERS vw ON v.VIEWERID = vw.VIEWERID
    JOIN MusicCompDimDB_c20424992.ParticipantDimension pd ON v.PARTNAME = pd.ParticipantName
GROUP BY
    v.EDITION_YEAR,
    v.PARTNAME,
    v.VOTEMODE,
    vw.CATEGORY,
    vw.AGE_GROUP,
    pd.CountyID;

-- C20424992 - An ETL process to populate the new VotesSummaryFact table
INSERT INTO
    MusicCompDimDB_c20424992.VotesSummaryFact (
        EditionYear,
        ParticipantName,
        CountyName,
        VoteMode,
        ViewerAgeGrp,
        TotalVotes
    )
SELECT vf.EditionYear, pd.ParticipantName, cd.CountyName, vf.VoteMode, vf.ViewerAgeGrp, SUM(vf.TotalVotes) AS TotalVotes
FROM MusicCompDimDB_c20424992.VotesFact vf
    JOIN MusicCompDimDB_c20424992.ParticipantDimension pd ON vf.ParticipantName = pd.ParticipantName
    JOIN MusicCompDimDB_c20424992.CountyDimension cd ON vf.CountyID = cd.CountyID
GROUP BY
    vf.EditionYear,
    pd.ParticipantName,
    cd.CountyName,
    vf.VoteMode,
    vf.ViewerAgeGrp;