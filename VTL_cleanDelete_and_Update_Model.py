# -*- coding: utf-8 -*-
"""
Modified extensively on 2021-03-23

## TN VTL Update Model ##

This script has been modified so it updates a layer in the SDE (SQLServer) Geodatabase 
and uses a stored map file to update a styled Vector Tile Package that is uploaded and
published as a Hosted Feature Layer in the Enterprise GIS portal.

The process which updates the SQLServer stored layer uses Pandas and PyODBC to
create a (Spatial) Data Frame and use fast/bulk writing to the NIWA GIS DB.

"""
import arcpy
from arcgis.gis import GIS
import os
import pandas as pd
import pyodbc
import shutil
from sys import argv

gis = GIS("https://portalgis.niwa.co.nz/arcgis", "portaladmin") # as we don't specify a PW, the script is going to prompt for it to be entered
sdeConn = "D:\\CONNECTIONS\\ADMIN@gis.sde"

indir = r"D:\TN_modelData_Download"
outdir = r"D:\TN_modelData_Convert"
notification_file = open(os.path.join(indir, "NOTIFICATION_TN-NATIONAL.txt"), "r") # the Manifest file to read for the uploaded NetCDFs
content = notification_file.read()

# we'll use a copy of the NetCDF File named in the notification text file for the conversion: 
infileName = os.path.join(indir, content.split("\n")[0])
outfileName = "streamq_allregions_CONVERT_for_webmap.nc"
infilePath = os.path.join(indir,infileName)
outFilePath = os.path.join(outdir,outfileName)

# Creating a SQLServer connection function for PyODBC 
def connectSQL(server,database,uid,pwd,auth="Private"):
    try:
        driverList = pyodbc.drivers()
        if auth == "Private":
            cnxn = pyodbc.connect("Driver={0};Server={1};Database={2};UID={3};PWD={4}".format(driverList[-1],server,database,uid,pwd))
        else:
            cnxn = pyodbc.connect("Driver={0};Server={1};Database={2};Trusted_Connection=yes;".format(driverList[-1],server,database))
    except:
        print("connectSQL: Cannot connect to database")
        raise
    else:
        print("Connected to the database successfully")
        return cnxn


def truncateTable(cnxn, tbl):
    try:
        curs = cnxn.cursor()
        curs.execute("TRUNCATE TABLE {0}".format(tbl))
    except pyodbc.DatabaseError as err:
        cnxn.rollback()
        print("truncateTable: Unable to truncate table - pyodbc.DatabaseError")
        raise err
    except Exception as e:
        cnxn.rollback()
        print("truncateTable: Unable to truncate table - unknown reason")
        raise e
    else:
        cnxn.commit()
        print("truncateTable successful")


def insertManyRecords(cnxn,dataColumns, dataInput, tableName):

    varHeaders = ','.join(dataColumns)   # Creating a comma delimited list 
    valsListVars = ','.join(['?'] * len(dataColumns))  # Creates a placeholder 
    
   
    print ("In function insertManyRecords now")

    
    try:
        cursor = cnxn.cursor()

        cnxn.autocommit = False
        cursor.fast_executemany = True
  
        sql_statement = "INSERT INTO {0}({1}) VALUES ({2})".format(tableName, varHeaders, valsListVars)
        print ("Running INSERT cursor: {0}".format(sql_statement))
        cursor.executemany(sql_statement, dataInput)
              
    except pyodbc.DatabaseError as err:
        cnxn.rollback()
        raise err
    except:
        cnxn.rollback()
        raise    
    else:
        cnxn.commit()

    print ("Insert has run")

    # Now we need to switch the view which is used to query the hydrograph to use our new table:
    try:
        cursor = cnxn.cursor()

        cnxn.autocommit = True
        cursor.fast_executemany = False
  
        sql_statement2 = '''    ALTER VIEW [GISADMIN].[TN_Rivers_Flow_All_NEW]
                                AS
                                SELECT        GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.OBJECTID, GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.rchid, 
                                                        GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.streamorder, GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.time, 
                                                        GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.nrch, GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.absoluteValues, 
                                                        GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.absoluteValues5thPercentile, GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.absoluteValues25thPercentile, 
                                                        GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.absoluteValuesMedian, GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.absoluteValues75thPercentile, 
                                                        GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.absoluteValues95thPercentile, GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.relativeValues, 
                                                        GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.relativeValues5thPercentile, GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.relativeValues25thPercentile, 
                                                        GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.relativeValuesMedian, GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.relativeValues75thPercentile, 
                                                        GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.relativeValues95thPercentile, NG.GISADMIN.rec1_RiverlinesStrahl_Order3_to7new_webMerc.Shape
                                FROM          GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B INNER JOIN
                                NG.GISADMIN.rec1_RiverlinesStrahl_Order3_to7new_webMerc ON
                                                        
                                                        GISADMIN.TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B.rchid =
                                                        NG.GISADMIN.rec1_RiverlinesStrahl_Order3_to7new_webMerc.Top_reach''' 
        print ("Running ALTER VIEW cursor: {0}".format(sql_statement2))
        cursor.execute(sql_statement2)
              
    except pyodbc.DatabaseError as err:
        cnxn.rollback()
        raise err
    except:
        cnxn.rollback()
        raise    
    else:
        cnxn.commit()
    print ("ALTER VIEW has run")

    # and also alter the other view which is used for the Vector Tile Layer compilation
    try:
        cursor = cnxn.cursor()

        cnxn.autocommit = True
        cursor.fast_executemany = False

        sql_statement3 = '''    ALTER VIEW [GISADMIN].[TN_Max_RelativeFlow]
                                    AS
                                    SELECT rchid,MAX(relativeValues) as relativeValues,streamorder from [GISADMIN].[TN_MODELVALUES_LAYER_NO_THRESHOLDS_TBL_B] 
                                    WHERE relativeValues >=0
                                    GROUP BY rchid,streamorder
        ''' 
        print ("Running ALTER VIEW cursor: {0}".format(sql_statement3))
        cursor.execute(sql_statement3)
              
    except pyodbc.DatabaseError as err:
        cnxn.rollback()
        raise err
    except:
        cnxn.rollback()
        raise    
    else:
        cnxn.commit()
    print ("ALTER VIEW for Vector Tile Layer has run")

def TNVTLupdateModel(GISADMIN_NG_welwmssql_sde=sdeConn, streamq_allregions_webmap_nc=outFilePath, TNRelativeFlowsVTL_vtpk="D:\\OneDrive - NIWA\\Projects\\Celine_Flood\\National_Flood\\MakeTNLayers_fromNetCDF\\Updates\\TNRelativeFlowsVTL_PROD.vtpk"):  # TN_VTL_Update_Model

    # The grunt of the work happens here in this function

    # To allow overwriting outputs change overwriteOutput option to True.
    arcpy.env.overwriteOutput = True
    
    print ("Copying NetCDF file from Download location to Conversion Folder")
    shutil.copy(infilePath,outFilePath)


    # Model Environment settings
    with arcpy.EnvManager(scratchWorkspace=r"D:\projects\floodData\Updates.gdb", workspace=r"D:\projects\floodData\Updates.gdb"):
        # This Map File stores the symbology etc. used for the Vector Tile Layer and is needed during every update.
        Input_Map = r"D:\projects\floodData\MakeTNLayers_fromNetCDF\Updates\TN_Rivers_VTL1.mapx"
                
        # Process: Make NetCDF Feature Layer (Make NetCDF Feature Layer) (md)
        ModelValues_Layer_TN = "ModelValues_Layer_TN"
        with arcpy.EnvManager(scratchWorkspace=r"D:\projects\floodData\Updates.gdb", workspace=r"D:\projects\floodData\Updates.gdb"):
            # tempLayer = arcpy.md.MakeNetCDFFeatureLayer(in_netCDF_file=streamq_allregions_webmap_nc, variable=["absoluteValues", "absoluteValues5thPercentile", "absoluteValues25thPercentile", "absoluteValuesMedian", "absoluteValues75thPercentile", "absoluteValues95thPercentile", "relativeValues", "relativeValues5thPercentile", "relativeValues25thPercentile", "relativeValuesMedian", "relativeValues75thPercentile", "relativeValues95thPercentile", "streamorder", "rchid"], x_variable="lon", y_variable="lat", out_feature_layer=ModelValues_Layer_TN, row_dimension=["nrch", "time"], z_variable="", m_variable="", dimension_values=[], value_selection_method="BY_VALUE")
            # print ("The Temp Feature Layer  has been created.")
            print ("Making the Temp Table View.")
            tempLayer2 = arcpy.md.MakeNetCDFTableView(in_netCDF_file=streamq_allregions_webmap_nc, variable=["absoluteValues", "absoluteValues5thPercentile", "absoluteValues25thPercentile", "absoluteValuesMedian", "absoluteValues75thPercentile", "absoluteValues95thPercentile", "relativeValues", "relativeValues5thPercentile", "relativeValues25thPercentile", "relativeValuesMedian", "relativeValues75thPercentile", "relativeValues95thPercentile", "streamorder", "rchid"],  out_table_view=ModelValues_Layer_TN, row_dimension=["nrch", "time"],  dimension_values=[], value_selection_method="BY_VALUE")
            print ("I've also made a Temp NetCDF Table View.")
            
            print ("Conversion to Pandas via Numpy non-spatial Data Frame is up next.")
            ##################### CONVERT TO  DATAFRAME ###############################
            nsdftab = arcpy.da.TableToNumPyArray(in_table=tempLayer2,field_names='*')
            
            # ARCHIVE -- Field specifications:
            # nsdftab = arcpy.da.TableToNumPyArray(in_table=tempLayer2,field_names=['nrch', 'time', 'absoluteValues', 'absoluteValues5thPercentile', 'absoluteValues25thPercentile', 'absoluteValuesMedian', 'absoluteValues75thPercentile', 'absoluteValues95thPercentile', 'relativeValues', 'relativeValues5thPercentile', 'relativeValues25thPercentile', 'relativeValuesMedian', 'relativeValues75thPercentile', 'relativeValues95thPercentile', 'streamorder', 'rchid'])
            print("Non-spatial NumpyArray created")
            nsdf = pd.DataFrame(nsdftab)
            
            
            
            #################### CONVERT TO LIST OF TUPLES | GET COLUMN NAME LIST##############################
            
            # Then to convert that sdf to a list of tuples:
            listTuplesFromSDF = list(nsdf.itertuples(index=False,name=None))

            # To get the list of column names it's:
            dataColumns = list(nsdf.columns)
            # To replace the first (OID) column with the correct (OBJECTID)
            dataColumns.pop(0)
            dataColumns.insert(0,"OBJECTID")
            
            try:
                print("Column names: {0}".format(dataColumns))
            except:
                print("not sure how to display column names in SDF.")


           
            # Execute SQL to insert the dataColumns
            
            try:
                # Variables
                tbl = 'NG.GISADMIN.TN_ModelValues_Layer_NO_thresholds_TBL_B'

                # Needs to be a list of tuples [(),(),()] that represents a row entry (let me know if you need help to convert the feature class to this format)
                # There must be a OBJECTID column and the SHAPE columns must be a geometry sql type
                # dataInput = [("1","Row1","Rowl2","Row3","geometry"),( "2", "Row1","Rowl2","Row3"," geometry"),( "3", "Row1","Rowl2","Row3"," geometry"),( "4", "Row1","Rowl2","Row3"," geometry"),( "5", "Row1","Rowl2","Row3"," geometry")]
                dataInput = listTuplesFromSDF

                # Connect to SQL - must change credentials:
                cnxn = connectSQL("myserver.mydomain.local","GIS","GISADMIN","MYPASSWORD")    # (INSTANCE,DATABASE,USER,PASSWORD)
                # Truncate Table
                truncateTable(cnxn, tbl)
                # Insert records into SQL
                insertManyRecords(cnxn,dataColumns, dataInput, tbl)
                #NG_GISADMIN_ModelValues_Layer_TN_NO_thresholds = cnxn

            except Exception as e:
                print("Insert function failed")
                raise e
            else:
                print("Main function successful")
            finally:
                cnxn.close()
                pass


        # Process: Feature Class to Feature Class (Feature Class to Feature Class) (conversion) - [needed as a one-off to begin with]
        with arcpy.EnvManager(scratchWorkspace=r"D:\projects\floodData\Updates.gdb", workspace=r"D:\projects\floodData\Updates.gdb"):
            #NG_GISADMIN_ModelValues_Layer_TN_NO_thresholds = arcpy.conversion.FeatureClassToFeatureClass(in_features=tempLayer, out_path=GISADMIN_NG_welwmssql_sde, out_name="TN_ModelValues_Layer_NO_thresholds", where_clause="", field_mapping="rchid \"rchid\" true true false 0 Long 0 0,First,#,ModelValues_Layer_TN,rchid,-1,-1;streamorder \"streamorder\" true true false 0 Long 0 0,First,#,ModelValues_Layer_TN,streamorder,-1,-1;nrch \"nrch\" true true false 0 Long 0 0,First,#,ModelValues_Layer_TN,nrch,-1,-1;time \"time\" true true false 0 Date 0 8,First,#,ModelValues_Layer_TN,time,-1,-1;absoluteValues \"absoluteValues\" true true false 0 Float 8 12,First,#,ModelValues_Layer_TN,absoluteValues,-1,-1;absoluteValues5thPercentile \"absoluteValues5thPercentile\" true true false 0 Float 8 12,First,#,ModelValues_Layer_TN,absoluteValues5thPercentile,-1,-1;absoluteValues25thPercentile \"absoluteValues25thPercentile\" true true false 0 Float 8 12,First,#,ModelValues_Layer_TN,absoluteValues25thPercentile,-1,-1;absoluteValuesMedian \"absoluteValuesMedian\" true true false 0 Float 8 12,First,#,ModelValues_Layer_TN,absoluteValuesMedian,-1,-1;absoluteValues75thPercentile \"absoluteValues75thPercentile\" true true false 0 Float 8 12,First,#,ModelValues_Layer_TN,absoluteValues75thPercentile,-1,-1;absoluteValues95thPercentile \"absoluteValues95thPercentile\" true true false 0 Float 8 12,First,#,ModelValues_Layer_TN,absoluteValues95thPercentile,-1,-1;relativeValues \"relativeValues\" true true false 0 Float 8 12,First,#,ModelValues_Layer_TN,relativeValues,-1,-1;relativeValues5thPercentile \"relativeValues5thPercentile\" true true false 0 Float 8 12,First,#,ModelValues_Layer_TN,relativeValues5thPercentile,-1,-1;relativeValues25thPercentile \"relativeValues25thPercentile\" true true false 0 Float 8 12,First,#,ModelValues_Layer_TN,relativeValues25thPercentile,-1,-1;relativeValuesMedian \"relativeValuesMedian\" true true false 0 Float 8 12,First,#,ModelValues_Layer_TN,relativeValuesMedian,-1,-1;relativeValues75thPercentile \"relativeValues75thPercentile\" true true false 0 Float 8 12,First,#,ModelValues_Layer_TN,relativeValues75thPercentile,-1,-1;relativeValues95thPercentile \"relativeValues95thPercentile\" true true false 0 Float 8 12,First,#,ModelValues_Layer_TN,relativeValues95thPercentile,-1,-1", config_keyword="")[0]
            print ("The Layer has been written into the DB.")
            

        # Process: Create Vector Tile Package (Create Vector Tile Package) (management)
        try:
            #if NG_GISADMIN_ModelValues_Layer_TN_NO_thresholds:
            print ("I'll now create a Vector Tile Package:")
            with arcpy.EnvManager(scratchWorkspace=r"D:\projects\floodData\Updates.gdb", workspace=r"D:\projects\floodData\Updates.gdb"):
                arcpy.management.CreateVectorTilePackage(in_map=Input_Map, output_file=TNRelativeFlowsVTL_vtpk, service_type="ONLINE", tiling_scheme="", tile_structure="INDEXED", min_cached_scale=295828763.7957775, max_cached_scale=564.248588, index_polygons="", summary="Layer displaying flow for a forecast period", tags="TN, River, Flow")
                print ("Creating VT Package successful. I'll delete the older Vector Tile Layer now before re-publishing.")
                search_result = gis.content.search("title:TNRelativeFlowsVTL_PROD", item_type = None)
                try:
                    search_result[0].delete()
                    search_result[1].delete()
                    print("old VTL title:TNRelativeFlowsVTL_PROD deleted")
                except:
                    print("nothing to delete")
                    
        except:
            print("unable to create VT Package")
            arcpy.GetMessages()

        # Process: Share Package (Share Package) (management)
        try:
            print ("Logged into {0}".format(gis))
            #gis.content.create_folder('TN')
            vtpk_item = gis.content.add({}, data=TNRelativeFlowsVTL_vtpk, folder='TN')
            print ("Added package successfully as {0}. Now sharing it publicly.".format(vtpk_item))
            vtile_layer = vtpk_item.publish()
            print ("Published package as tiled service successfully as {0}".format(vtile_layer))
            try:
                vtile_layer.share(everyone=True)
            except:
                print("couldn't share the layer publicly")

        except:
            print("Publishing VTL has failed  {0} ".format(arcpy.GetMessages()))
            
if __name__ == '__main__':
    TNVTLupdateModel(*argv[1:])
    print("All done.   {0} ".format(arcpy.GetMessages()))