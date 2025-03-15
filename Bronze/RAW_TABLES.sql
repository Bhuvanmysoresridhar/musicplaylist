CREATE OR REPLACE FILE FORMAT csv_file_format
  TYPE=CSV
  FIELD_DELIMITER=','
  SKIP_HEADER=1
  NULL_IF=('NULL','null','')
  EMPTY_FIELD_AS_NULL=TRUE
  COMPRESSION=AUTO;

  
---TABLE RAW_TRACK 
CREATE TABLE IF NOT EXISTS RAW_TRACK(
    TrackId	INT PRIMARY KEY,
    Name STRING,
    AlbumId	INT ,
    MediaTypeId	INT,
    GenreId	INT,
    Composer STRING,	
    Milliseconds INT,
    Bytes INT,
    UnitPrice FLOAT,
    source_file_name STRING,
    source_file_row_number INT,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY(AlbumId) REFERENCES RAW_Album(Album_Id),
    FOREIGN KEY(MediaTypeId) REFERENCES RAW_MediaType(MediaType_Id),
    FOREIGN KEY(GenreId) REFERENCES RAW_Genre(GenreId)
)

CREATE OR REPLACE TASK load_track_data
   WAREHOUSE=compute_wh
   SCHEDULE='1 MINUTE'
AS
   COPY INTO RAW_TRACK(
    TrackId,
    Name,
    AlbumId,	
    MediaTypeId,	
    GenreId,
    Composer, 	
    Milliseconds, 
    Bytes,
    UnitPrice, 
    source_file_name, 
    source_file_row_number, 
    ingestion_timestamp
)
    FROM (
    SELECT
       $1,
       $2,
       $3,
       $4,
       $5,
       $6,
       $7,
       $8,
       $9,
       metadata$filename,
       metadata$file_row_number,
       CURRENT_TIMESTAMP()
       FROM @adls_stage/Track/Track.csv
)
     FILE_FORMAT=(FORMAT_NAME='csv_file_format')
     ON_ERROR='CONTINUE'
     PATTERN='.*[.]csv'

ALTER TASK load_track_data RESUME;

select * from raw_track
ORDER BY TRACKID;

EXECUTE TASK load_track_data;

-----

---TABLE RAW_PLAYLISTTRACK

CREATE TABLE IF NOT EXISTS RAW_PLAYLISTTRACK(
    PLAYLIST_ID INT,
    TRACK_ID INT,
    PRIMARY KEY (PLAYLIST_ID,TRACK_ID),
    FOREIGN KEY (PLAYLIST_ID) REFERENCES RAW_PLAYLIST(PLAYLIST_ID),
    FOREIGN KEY (TRACK_ID) REFERENCES RAW_TRACK(TrackId)
)


CREATE OR REPLACE TASK Load_playlisttrack_data
    WAREHOUSE = compute_wh
    SCHEDULE ='1 MINUTE'
AS
    COPY INTO RAW_PLAYLISTTRACK(
        PLAYLIST_ID,
        TRACK_ID
        )
       FROM (
       SELECT
         $1,
         $2
         FROM @adls_stage/PlaylistTrack/PlaylistTrack.csv
       )
       FILE_FORMAT=(FORMAT_NAME='csv_file_format')
       ON_ERROR='CONTINUE'
       PATTERN = '.*';

       ALTER TASK Load_playlisttrack_data RESUME;

       EXECUTE TASK load_track_data;

       select * from RAW_PLAYLISTTRACK;

----

---TABLE RAW_PLAYLIST

   CREATE TABLE IF NOT EXISTS RAW_PLAYLIST(
       PLAYLIST_ID INT PRIMARY KEY ,
       PLAYLIST_NAME VARCHAR(500)
   )
   
   CREATE OR REPLACE TASK Load_playlist_data
        WAREHOUSE= compute_wh
        SCHEDULE = '1 MINUTE'
    AS
        COPY INTO RAW_PLAYLIST(
            PLAYLIST_ID,
            PLAYLIST_NAME
        )
        FROM (
        SELECT 
          $1,
          $2
          FROM @adls_stage/Playlist/Playlist.csv
        )
        FILE_FORMAT=(FORMAT_NAME='csv_file_format')
        ON_ERROR='CONTINUE'
        PATTERN = '.*';

        ALTER TASK Load_playlist_data RESUME;

        EXECUTE TASK Load_playlist_data; 

        SELECT * FROM RAW_PLAYLIST;
----

        ---TABLE RAW_Media_Type
        
        CREATE TABLE IF NOT EXISTS RAW_MediaType(
           MediaType_Id INT PRIMARY KEY,
           Name VARCHAR(500)
        )
        
        CREATE OR REPLACE TASK Load_MediaType_data
            WAREHOUSE = compute_wh
            SCHEDULE = '1 MINUTE'

        AS
           COPY INTO RAW_MediaType(
              MediaType_Id,
              Name
           )
        FROM (
        SELECT
          $1,
          $2
          FROM @adls_stage/MediaType/MediaType.csv
        )
        FILE_FORMAT=(FORMAT_NAME='csv_file_format')
        ON_ERROR='CONTINUE'
        PATTERN = '.*';

        ALTER TASK Load_MediaType_data RESUME;

        EXECUTE TASK Load_MediaType_data;

        SELECT * FROM RAW_MediaType;
----

        ---TABLE RAW_InvoiceLine
        
        CREATE TABLE IF NOT EXISTS RAW_InvoiceLine(
              InvoiceLineId	INT PRIMARY KEY,
              InvoiceId	INT,
              TrackId INT,
              UnitPrice	FLOAT,
              Quantity INT,
              FOREIGN KEY (InvoiceId) REFERENCES RAW_Invoice(InvoiceId),
              FOREIGN KEY (TrackId) REFERENCES RAW_TRACK(TrackId)
        );
           
            
        CREATE OR REPLACE TASK Load_InvoiceLine_data
            WAREHOUSE = compute_wh
            SCHEDULE = '1 MINUTE'
        AS
           COPY INTO RAW_InvoiceLine(
               InvoiceLineId,
               InvoiceId,
               TrackId,
               UnitPrice,
               Quantity
           )

        FROM (
          SELECT
            $1,
            $2,
            $3,
            $4,
            $5
            FROM @adls_stage/InvoiceLine/InvoiceLine.csv
        )
        FILE_FORMAT=(FORMAT_NAME='csv_file_format')
        ON_ERROR='CONTINUE'
        PATTERN = '.*';

        ALTER TASK Load_InvoiceLine_data RESUME;

        EXECUTE TASK Load_InvoiceLine_data;

        SELECT * FROM RAW_INVOICELINE;

---

--- TABLE RAW_Invoice
        
        CREATE TABLE IF NOT EXISTS RAW_Invoice(
            InvoiceId INT PRIMARY KEY,	
            CustomerId INT,	
            InvoiceDate TIMESTAMP_NTZ,	
            BillingAddress VARCHAR(700),	
            BillingCity VARCHAR(500),	
            BillingState VARCHAR(500),	
            BillingCountry VARCHAR(500),	
            BillingPostalCode INT,	
            Total FLOAT,
            FOREIGN KEY(CustomerId) REFERENCES RAW_Customer(CustomerId)
        )
    
        CREATE OR REPLACE TASK Load_Invoice_data 
             WAREHOUSE = compute_wh
             SCHEDULE = '1 MINUTE'
        AS
            COPY INTO RAW_Invoice(
              InvoiceId ,	
              CustomerId ,	
              InvoiceDate,	
              BillingAddress,	
              BillingCity,	
              BillingState,	
              BillingCountry,	
              BillingPostalCode,	
              Total
            )
            FROM (
             SELECT 
               $1,
               $2,
               $3,
               $4,
               $5,
               $6,
               $7,
               $8,
               $9
               FROM @adls_stage/Invoice/Invoice.csv
            )
            FILE_FORMAT=(FORMAT_NAME='csv_file_format')
            ON_ERROR='CONTINUE'
            PATTERN = '.*';

            ALTER TASK Load_Invoice_data RESUME;

            EXECUTE TASK Load_Invoice_data ;

            SELECT * FROM RAW_Invoice;
----

----TABLE RAW_Genre

            CREATE TABLE IF NOT EXISTS RAW_Genre(
                   GenreId INT PRIMARY KEY,
                   Name VARCHAR(200)
            )

            
            CREATE OR REPLACE TASK Load_Genre_data 
                WAREHOUSE = compute_wh
                SCHEDULE = '1 MINUTE'
            AS
               COPY INTO RAW_Genre(
                   GenreId,
                   Name
               )
               FROM (
                SELECT
                  $1,
                  $2
                  FROM @adls_stage/Genre/Genre.csv
               )
               FILE_FORMAT=(FORMAT_NAME='csv_file_format')
               ON_ERROR='CONTINUE'
               PATTERN = '.*';

               ALTER TASK Load_Genre_data RESUME;

               EXECUTE TASK Load_Genre_data ;

               SELECT * FROM RAW_Genre;
----

---TABLE RAW_Employee
              
               CREATE TABLE IF NOT EXISTS RAW_Employee(
                 EmployeeId INT PRIMARY KEY,	
                 FirstName VARCHAR(200),
                 LastName  VARCHAR(200),
                 Title	VARCHAR(300),
                 ReportsTo	INT,
                 BirthDate	TIMESTAMP_NTZ,
                 HireDate  TIMESTAMP_NTZ,	
                 Address VARCHAR(300),
                 City  VARCHAR(100),
                 State	VARCHAR(100),
                 Country VARCHAR(100),
                 PostalCode	VARCHAR(100),
                 Phone	VARCHAR(50),
                 Fax	VARCHAR(50),
                 Email VARCHAR(100)
               )


               CREATE OR REPLACE TASK Load_Employee_data
                   WAREHOUSE=compute_wh
                   SCHEDULE='1 MINUTE'
                AS
                    COPY INTO RAW_Employee(
                       EmployeeId,
                       FirstName,
                       LastName, 
                       Title,
                       ReportsTo,
                       BirthDate,
                       HireDate,
                       Address,
                       City,
                       State,
                       Country,
                       PostalCode,
                       Phone,
                       Fax,
                       Email
                    )

                 FROM (
                    SELECT 
                       $1,
                       $2,
                       $3,
                       $4,
                       $5,
                       $6,
                       $7,
                       $8,
                       $9,
                       $10,
                       $11,
                       $12,
                       $13,
                       $14,
                       $15
                       FROM @adls_stage/Employee/Employee.csv
                       )
                       FILE_FORMAT=(FORMAT_NAME='csv_file_format')
                       ON_ERROR='CONTINUE'
                       PATTERN = '.*[.]csv';

                       ALTER TASK Load_Employee_data RESUME;

                       EXECUTE TASK Load_Employee_data;

                       SELECT * FROM RAW_EMPLOYEE;
-----

---TABLE RAW_Customer

        CREATE TABLE IF NOT EXISTS RAW_Customer(
              CustomerId INT PRIMARY KEY,
              FirstName	VARCHAR(100),
              LastName	VARCHAR(100),
              Company  VARCHAR(300),
              Address	VARCHAR(300),
              City   STRING,
              State	STRING,
              Country STRING,	
              PostalCode VARCHAR(20),	
              Phone	VARCHAR(50),
              Fax  VARCHAR(50),	
              Email	VARCHAR(100),
              SupportRepId INT
        );
        
        CREATE OR REPLACE TASK Load_Customer_data
           WAREHOUSE=compute_wh
           SCHEDULE='1 MINUTE'

        AS
            COPY INTO RAW_Customer(
               CustomerId,
               FirstName,
               LastName,
               Company,
               Address,
               City,
               State,
               Country,
               PostalCode,
               Phone,
               Fax,
               Email,
               SupportRepId
            )
        FROM ( 
           SELECT 
           $1,
           $2,
           $3,
           $4,
           $5,
           $6,
           $7,
           $8,
           $9,
           $10,
           $11,
           $12,
           $13
           FROM @adls_stage/Customer/Customer.csv
        )
        FILE_FORMAT=(FORMAT_NAME='csv_file_format')
        ON_ERROR='CONTINUE'
        PATTERN = '.*[.]csv';

      ALTER TASK Load_Customer_data RESUME;

      EXECUTE TASK Load_Customer_data;

      SELECT * FROM RAW_Customer;


      ALTER FILE FORMAT MUSIC_PLAYLIST_DB.BRONZE.CSV_FILE_FORMAT 
      SET ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

      ALTER FILE FORMAT MUSIC_PLAYLIST_DB.BRONZE.CSV_FILE_FORMAT 
      SET 
      ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE,
      MULTI_LINE = TRUE,
      SKIP_BLANK_LINES = TRUE,
      TRIM_SPACE = TRUE;

      
      SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13 
      FROM @adls_stage/Customer/Customer.csv
      (FILE_FORMAT => 'CSV_FILE_FORMAT');

     ------
     CREATE TABLE IF NOT EXISTS RAW_ARTIST(
           Artist_Id INT PRIMARY KEY,
           Name VARCHAR(100)
      )
      
      CREATE OR REPLACE TASK Load_Artist_data
         WAREHOUSE = compute_wh
         SCHEDULE = '1 MINUTE'
    AS
      COPY INTO RAW_ARTIST(
        Artist_Id,
        Name
     )  
     FROM (
       SELECT
       $1,
       $2
       FROM @adls_stage/Artist/Artist.csv
     )
     FILE_FORMAT=(FORMAT_NAME='csv_file_format')
     ON_ERROR='CONTINUE'
     PATTERN = '.*[.]csv';

     ALTER TASK Load_Artist_data RESUME;

     EXECUTE TASK Load_Artist_data;

     SELECT * FROM RAW_ARTIST;

-----

     ---TABLE RAW_Album
     CREATE TABLE IF NOT EXISTS RAW_Album(
         Album_Id INT PRIMARY KEY,
         Tiltle VARCHAR(300),
         Artist_Id INT,
         FOREIGN KEY (Artist_Id) REFERENCES RAW_ARTIST(Artist_Id)
        )

    CREATE OR REPLACE TASK Load_Album_data
         WAREHOUSE=compute_wh
         SCHEDULE='1 MINUTE'
    AS 
       COPY INTO RAW_Album(
         Album_Id,
         Tiltle,
         Artist_Id
       )
       FROM (
        SELECT
         $1,
         $2,
         $3,
         FROM @adls_stage/Album/Album.csv
       )
       FILE_FORMAT=(FORMAT_NAME='csv_file_format')
       ON_ERROR='CONTINUE'
       PATTERN = '.*[.]csv';

       ALTER TASK Load_Album_data RESUME;
       EXECUTE TASK Load_Album_data;
       SELECT * FROM RAW_Album;




