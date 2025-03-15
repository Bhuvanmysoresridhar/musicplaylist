CREATE OR REPLACE STORAGE INTEGRATION azure_musicplaylist_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  AZURE_TENANT_ID = '93d7dcd5-dbb9-49c5-bcbd-194b5aea72b0'
  STORAGE_ALLOWED_LOCATIONS = ('azure://musicplaylist.blob.core.windows.net/landing/')
  ENABLED = TRUE;

  DESC STORAGE INTEGRATION azure_musicplaylist_integration

  USE MUSIC_PLAYLIST_DB.BRONZE;
  CREATE OR REPLACE STAGE adls_stage
     STORAGE_INTEGRATION= azure_musicplaylist_integration
     URL='azure://musicplaylist.blob.core.windows.net/landing'

ls@adls_stage
    
---VERIFY
  SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9
      FROM @adls_stage/Track
  (FILE_FORMAT => csv_file_format)
      LIMIT 10;