-- Drop and create the schema

--Change the schema name to be your student number
DROP SCHEMA IF EXISTS MusicCompDimDB CASCADE;
CREATE SCHEMA MusicCompDimDB;
SET SEARCH_PATH TO MusicCompDimDB;

-- Create Dimension Tables

-- Edition Dimension
CREATE TABLE EditionDimension (
    EditionYear INT UNIQUE PRIMARY KEY, 
    PresenterName VARCHAR(100)
);

-- Participant Dimension
CREATE TABLE ParticipantDimension (
    ParticipantName VARCHAR(100) PRIMARY KEY,  
    CountyName VARCHAR(100)  
);

-- Viewer Agegroup Dimension
CREATE TABLE AgeDimension (
  AGE_GROUPID INTEGER NOT NULL,
  AGE_GROUP_DESC VARCHAR(50) DEFAULT NULL,
  PRIMARY KEY (AGE_GROUPID)
);
-- Viewer Category Dimension
CREATE TABLE CategoryDimension (
    CatID INT PRIMARY KEY,  
    CatName VARCHAR(50)  DEFAULT NULL CHECK (CATNAME IN ('Jury', 'Audience'))
);

-- Create Fact Table (Votes by Vote Mode from Viewers in Each Category, AgeGroup in Each County)
CREATE TABLE VotesFact (
    EditionYear INT,
    ParticipantName VARCHAR(100),
    VoteMode VARCHAR(10),
	ViewerCat INT,
	ViewerAgeGrp INT,
    TotalVotes INT,  -- Aggregated count of votes
    PRIMARY KEY (EditionYear, ParticipantName, ViewerCat, ViewerAgeGrp, VoteMode),
    FOREIGN KEY (EditionYear) REFERENCES EditionDimension(EditionYear),
    FOREIGN KEY (ParticipantName) REFERENCES ParticipantDimension(ParticipantName),
    FOREIGN KEY (ViewerCat) REFERENCES CategoryDimension(CatID),
    FOREIGN KEY (ViewerAgeGrp) REFERENCES AgeDimension(AGE_GROUPID)
);

-- Populate Edition Dimension
INSERT INTO MusicCompDimDB.EditionDimension (EditionYear, PresenterName)
SELECT EDYEAR, EDPRESENTER
FROM MusicCompDB.Edition;

-- Populate Participant Dimension
INSERT INTO MusicCompDimDB.ParticipantDimension (ParticipantName, CountyName)
SELECT p.PARTNAME, c.COUNTYNAME
FROM MusicCompDB.PARTICIPANTS p
LEFT JOIN MusicCompDB.COUNTY c ON p.COUNTYID = c.COUNTYID;

-- Populate Age Group Dimension
INSERT INTO MusicCompDimDB.AgeDimension (AGE_GROUPID, AGE_GROUP_DESC)
SELECT AGE_GROUPID, AGE_GROUP_DESC
FROM MusicCompDB.AGEGROUP;

-- Populate the Category Dimension
INSERT INTO MusicCompDimDB.CategoryDimension (CatID, CatName)
SELECT CATID, CATNAME
FROM MusicCompDB.VIEWERCATEGORY;

