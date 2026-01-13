
USE ODS
GO


CREATE OR ALTER PROCEDURE [ABMRetro].[usp_EDI_Gateway_X12_Processing_Event_Driver]
	@Topic varchar(255),	-- Must be a parameter because topic can change per environment
	@Debug bit = 0
AS 

/*****************************************************
* Name:		ABMRetro.usp_EDI_Gateway_X12_Processing_Event_Driver
* Purpose:  BIO-1277
*           Loops through new events and executes appropriate stored procedure to load ABMRetro tables
* Inputs:    
* Returns:             
* Created By:	Mike Salzman
* Created Date:	2/19/2025
* Modified By	: Mike Salzman
* Purpose		: BIO-1997 - Remove dependency on offset for uniqueness
* Modified Date	: 1/9/2025
******************************************************/	

BEGIN

    SET NOCOUNT ON;
    SET ANSI_PADDING ON;
    SET ANSI_WARNINGS ON;
    SET IMPLICIT_TRANSACTIONS OFF;
    SET CURSOR_CLOSE_ON_COMMIT OFF; 
    SET ARITHABORT ON;
    SET CONCAT_NULL_YIELDS_NULL ON;
    SET NUMERIC_ROUNDABORT OFF;
    SET XACT_ABORT OFF;

	BEGIN TRY

		-- Create table variable to store new events
		DECLARE @Retro_Event_Log table (
			ID int IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED,
			Retro_Event_Log_Id bigint,
			Message_Type varchar(100)
		)

		INSERT INTO @Retro_Event_Log (
			Retro_Event_Log_Id,
			Message_Type
		)
		SELECT		Retro_Event_Log_Id,
					Message_Type
		FROM		ABMRetro.Retro_Event_Log
		WHERE		Process_Status = 'Loaded'
		AND			Topic = @Topic
		AND			Message_Type IN (
						-- Limit to event types that we are currently handling
						'RetroFileReceivedEvent',
						'RetroFileDeliveredEvent',
						'RetroFileProcessedEvent',
						'RetroFileProcessingFailedEvent'
					)
		ORDER BY	CASE
						WHEN Topic_Key IS NULL THEN Event_Timestamp	-- If Topic_Key is NOT populated, the event timestamp is the safest route to guarantee order
						ELSE NULL
					END ASC,
					Topic_Partition_Num ASC,
					Topic_Offset_Num ASC


		-- Set up variables for loop
		DECLARE @Current_ID int = 1,
				@Max_ID int = 1,
				@Current_Retro_Event_Log_Id bigint,
				@Current_Message_Type varchar(100),
				@SQL varchar(MAX),
				@Duplicate_Flag bit = 0

		SELECT	@Max_ID = Max(ID)
		FROM	@Retro_Event_Log
		


		-- Loop through each new event
		WHILE @Current_ID <= @Max_ID
		BEGIN

			-- Set event variables for current loop iteration
			SELECT	@Current_Retro_Event_Log_Id = Retro_Event_Log_Id,
					@Current_Message_Type = Message_Type
			FROM	@Retro_Event_Log
			WHERE	ID = @Current_ID
				

			-- Check if current event is a duplicate
			IF EXISTS (
				SELECT		*
				FROM		ABMRetro.Retro_Event_Log l
				INNER JOIN	ABMRetro.Retro_Event_Log l_dupes
								ON	l.Retro_Event_Log_ID <> l_dupes.Retro_Event_Log_ID
								AND l.Topic = l_dupes.Topic
								AND l.Message_Type = l_dupes.Message_Type
								AND l.Event_Message_Hash = l_dupes.Event_Message_Hash
				WHERE		l.Retro_Event_Log_ID = @Current_Retro_Event_Log_Id
			)
			BEGIN
				SET @Duplicate_Flag = 1
			END
			ELSE
			BEGIN
				SET @Duplicate_Flag = 0
			END



			-- Dynamically set name of proc based on event type
			-- Note: All procs MUST be named "ABMRetro.usp_EDI_Gateway_X12_Processing_" followed by the message type handled by that proc
			SELECT @SQL =
				CASE
					WHEN @Duplicate_Flag = 1 THEN '-- Duplicate detected for Retro_Event_Log_Id ' + Convert(varchar(20), @Current_Retro_Event_Log_Id)
					ELSE 'EXECUTE ABMRetro.usp_EDI_Gateway_X12_Processing_' + @Current_Message_Type + ' @Retro_Event_Log_Id = ' + Convert(varchar(20), @Current_Retro_Event_Log_Id)
				END
	

			IF @Debug = 1
				PRINT 'Now executing:  ' + @SQL


			BEGIN TRY
				BEGIN TRANSACTION;

					-- Execute event type proc
					EXECUTE(@SQL);


					-- Update status
					UPDATE	el
					SET		el.Process_Status = 
								CASE
									WHEN @Duplicate_Flag = 1 THEN 'Duplicate'
									ELSE 'Success'
								END
					FROM	ABMRetro.Retro_Event_Log el
					WHERE	el.Retro_Event_Log_Id = @Current_Retro_Event_Log_Id


					IF @@TRANCOUNT > 0
						COMMIT;

				END TRY
				BEGIN CATCH

					IF @@TRANCOUNT > 0
						ROLLBACK TRANSACTION;

					DECLARE	@ErrorMessage nvarchar(4000) = 'Error message: ' + ERROR_MESSAGE() + Char(13) + Char(10) + 'Error executing the following statement:  ' + @SQL,
							@ErrorSeverity int = ERROR_SEVERITY(),
							@ErrorNumber int = ERROR_NUMBER()

					RAISERROR ( @ErrorMessage, @ErrorSeverity, 1, @ErrorNumber );

					BREAK;	-- Exit loop if an error occurs so no further events are processed
				END CATCH

				-- Increment loop ID
				SET @Current_ID = @Current_ID + 1

		END


	END TRY
	BEGIN CATCH
	   -- Output for debug 
	   IF @Debug = 1
		  PRINT 'In CATCH block.
		  Error number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10)) + '
		  Error message: ' + ERROR_MESSAGE() + '
		  Error severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10)) + '
		  Error state: ' + CAST(ERROR_STATE() AS VARCHAR(10)) + '
		  XACT_STATE: ' + CAST(XACT_STATE() AS VARCHAR(10))

		-- Calling a utility stored procedure to return the error information to the application 
		EXECUTE usp_util_RethrowError;

    
	END CATCH

END
GO