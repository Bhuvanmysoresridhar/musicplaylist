create or replace TABLE MUSIC_PLAYLIST_DB.SILVER.DIM_ALBUM_ARTIST (
	ALBUMID NUMBER(38,0) NOT NULL,
	TITLE VARCHAR(300),
	ARTIST_NAME VARCHAR(200),
	primary key (ALBUMID)
);

INSERT INTO MUSIC_PLAYLIST_DB.SILVER.DIM_ALBUM_ARTIST (ALBUMID, TITLE, ARTIST_NAME)
SELECT 
    Alb.ALBUM_ID,  
    Alb.TILTLE,  
    Ar.NAME  
FROM MUSIC_PLAYLIST_DB.BRONZE.RAW_ALBUM AS Alb  
INNER JOIN MUSIC_PLAYLIST_DB.BRONZE.RAW_ARTIST AS Ar  
   ON Alb.ARTIST_ID = Ar.ARTIST_ID  
WHERE Alb.TILTLE IS NOT NULL  
      AND Ar.NAME IS NOT NULL
GROUP BY Alb.ALBUM_ID, Alb.TILTLE, Ar.NAME;

select * from DIM_ALBUM_ARTIST
ORDER BY ALBUMID;

----

create or replace TABLE MUSIC_PLAYLIST_DB.SILVER.DIM_Playlist (
	PLAYLIST_ID NUMBER(38,0) NOT NULL,
	NAME VARCHAR(300),
    TRACKID NUMBER(38,0) NOT NULL,
	primary key (PLAYLIST_ID)
);

INSERT INTO MUSIC_PLAYLIST_DB.SILVER.DIM_Playlist (PLAYLIST_ID, NAME, TRACKID)
SELECT 
   CASE 
     WHEN Pl.PLAYLIST_ID = 8 THEN 1 
     ELSE Pl.PLAYLIST_ID 
   END,
   Pl.PLAYLIST_NAME,
   Plt.TRACK_ID
FROM  MUSIC_PLAYLIST_DB.BRONZE.RAW_PLAYLIST AS Pl
INNER JOIN MUSIC_PLAYLIST_DB.BRONZE.RAW_PLAYLISTTRACK AS Plt
      ON Pl.PLAYLIST_ID = Plt.PLAYLIST_ID;

CREATE OR REPLACE TABLE MUSIC_PLAYLIST_DB.SILVER.DIM_TRACK(
    TrackId INT NOT NULL,	
    AlbumId	INT,
    Name VARCHAR(300),
    Composer VARCHAR(100),
    Media_Type_Name VARCHAR(100),
    Genre_Name VARCHAR(100),
    Milliseconds INT,	
    Bytes	INT,
    UnitPrice FLOAT,
    PRIMARY KEY(TrackId),
    FOREIGN KEY (AlbumId) REFERENCES DIM_ALBUM_ARTIST(ALBUMID)
);


INSERT INTO MUSIC_PLAYLIST_DB.SILVER.DIM_TRACK(TrackId,AlbumId,Name,Composer,Media_Type_Name,Genre_Name,Milliseconds,Bytes,UnitPrice)
SELECT DISTINCT
   Tk.TRACKID,
   Tk.ALBUMID,
   Tk.NAME,
   Tk.COMPOSER,
   Media.NAME,
   Gen.NAME,
   Tk.Milliseconds,
   Tk.Bytes,
   Tk.UnitPrice
FROM MUSIC_PLAYLIST_DB.BRONZE.RAW_TRACK AS Tk
INNER JOIN  MUSIC_PLAYLIST_DB.BRONZE.RAW_MEDIATYPE AS Media 
    ON Tk.MEDIATYPEID = Media.MEDIATYPE_ID
INNER JOIN  MUSIC_PLAYLIST_DB.BRONZE.RAW_GENRE AS Gen 
    ON Tk.GENREID = Gen.GENREID
WHERE Tk.COMPOSER IS NOT NULL 

ALTER TABLE MUSIC_PLAYLIST_DB.SILVER.DIM_TRACK 
ALTER COLUMN Composer SET DATA TYPE VARCHAR(500);

select * from DIM_TRACK 
ORDER BY TRACKID;

----

CREATE OR REPLACE TABLE MUSIC_PLAYLIST_DB.SILVER.DIM_CUSTOMER(
    CustomerId INT PRIMARY KEY,	
    FirstName VARCHAR(200),
    LastName VARCHAR(200),
    Address	VARCHAR(300),
    City VARCHAR(200),
    State VARCHAR(100),
    Country	VARCHAR(50),
    PostalCode VARCHAR(100),
    Phone VARCHAR(100),
    Email VARCHAR(100),
    SupportRepId INT,
    FOREIGN KEY (SupportRepId) REFERENCES DIM_EMPLOYEES(EmployeeId)
);


INSERT INTO MUSIC_PLAYLIST_DB.SILVER.DIM_CUSTOMER(
    CustomerId, FirstName, LastName, Address, City, State, Country, PostalCode, Phone, Email, SupportRepId
)
SELECT DISTINCT
    CUSTOMERID,
    FIRSTNAME,
    LASTNAME,
    ADDRESS,
    CITY,
    CASE 
        WHEN STATE IS NULL THEN 'UNKNOWN'
        ELSE STATE
    END AS STATE,
    COUNTRY,
    CASE 
        WHEN CITY = 'Lisbon' THEN '1000'  -- Convert to string
        WHEN CITY = 'Porto' THEN '4000'  -- Convert to string
        WHEN CITY = 'Dublin' THEN 'D02XY12'  -- String value
        WHEN CITY = 'Santiago' THEN '8320000'  -- Convert to string
        ELSE POSTALCODE
    END AS POSTALCODE,
    PHONE,
    EMAIL,
    SUPPORTREPID
FROM MUSIC_PLAYLIST_DB.BRONZE.RAW_CUSTOMER;

-----

CREATE OR REPLACE TABLE MUSIC_PLAYLIST_DB.SILVER.DIM_EMPLOYEES(
     EmployeeId	INT PRIMARY KEY,	
     FirstName	VARCHAR(100),
     LastName VARCHAR(100),
     Title	VARCHAR(300),
     ReportsTo INT,
     BirthDate DATE, 	
     HireDate DATE,
     Address VARCHAR(300),	
     City VARCHAR(100),	
     State	VARCHAR(100),
     Country VARCHAR(100),	
     PostalCode	VARCHAR(100),
     Phone	VARCHAR(40),	
     Email VARCHAR(100)
);

INSERT INTO MUSIC_PLAYLIST_DB.SILVER.DIM_EMPLOYEES(EmployeeId,FirstName,LastName,Title,ReportsTo,BirthDate,HireDate,Address,City,State,Country,PostalCode,Phone,Email)
SELECT 
   EMPLOYEEID,
   FIRSTNAME,
   LASTNAME,
   TITLE,
   REPORTSTO,
   DATE(BIRTHDATE),
   DATE(HIREDATE),
   ADDRESS,
   CITY,
   STATE,
   COUNTRY,
   POSTALCODE,
   PHONE,
   EMAIL
FROM MUSIC_PLAYLIST_DB.BRONZE.RAW_EMPLOYEE;

----
CREATE OR REPLACE TABLE MUSIC_PLAYLIST_DB.SILVER.DIM_INVOICE (
   InvoiceId INT PRIMARY KEY ,	
   CustomerId INT ,		
   InvoiceDate DATE,
   BillingAddress VARCHAR(300),	
   BillingCity	VARCHAR(100),
   BillingState	VARCHAR(100),
   BillingCountry VARCHAR(100),
   BillingPostalCode VARCHAR(100),	
   Total FLOAT,
   FOREIGN KEY (CustomerId) REFERENCES DIM_CUSTOMER(CustomerId)
);

INSERT INTO MUSIC_PLAYLIST_DB.SILVER.DIM_INVOICE(
    InvoiceId, CustomerId, InvoiceDate, BillingAddress, BillingCity, BillingState,
    BillingCountry, BillingPostalCode, Total
)
SELECT DISTINCT
    InvoiceId,	
    CustomerId,	
    DATE(InvoiceDate),	
    BillingAddress,	
    BillingCity,	
    CASE 
        WHEN BillingState IS NULL THEN 'UNKNOWN'  
        ELSE BillingState
    END AS BillingState,
    BillingCountry,	
    BillingPostalCode,	
    Total
FROM MUSIC_PLAYLIST_DB.BRONZE.RAW_INVOICE;

----

CREATE OR REPLACE TABLE MUSIC_PLAYLIST_DB.SILVER.DIM_INVOICELINE(
   InvoiceLine_Id INT PRIMARY KEY,
   InvoiceId INT ,
   TrackId INT,
   Unit_Price FLOAT,
   Quantity INT,
   FOREIGN KEY (InvoiceId) REFERENCES DIM_INVOICE(InvoiceId),
   FOREIGN KEY (TrackId) REFERENCES DIM_TRACK(TrackId)
);

INSERT INTO MUSIC_PLAYLIST_DB.SILVER.DIM_INVOICELINE(InvoiceLine_Id,InvoiceId,TrackId,Unit_Price,Quantity)
SELECT DISTINCT
  InvoiceLineId,
  InvoiceId	,
  TrackId,
  UnitPrice,	
  Quantity
FROM MUSIC_PLAYLIST_DB.BRONZE.RAW_INVOICELINE;


