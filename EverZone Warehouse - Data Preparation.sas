session server;

/* Start checking for existence of each input table */
exists0=doesTableExist("CASUSER(kelly.mae@student.umn.ac.id)", "EVERZONE WAREHOUSE DATASET");
if exists0 == 0 then do;
  print "Table "||"CASUSER(kelly.mae@student.umn.ac.id)"||"."||"EVERZONE WAREHOUSE DATASET" || " does not exist.";
  print "UserErrorCode: 100";
  exit 1;
end;
print "Input table: "||"CASUSER(kelly.mae@student.umn.ac.id)"||"."||"EVERZONE WAREHOUSE DATASET"||" found.";
/* End checking for existence of each input table */


  _dp_inputTable="EVERZONE WAREHOUSE DATASET";
  _dp_inputCaslib="CASUSER(kelly.mae@student.umn.ac.id)";

  _dp_outputTable="4c7208d2-f0a0-44c4-9eb9-754740090822";
  _dp_outputCaslib="CASUSER(kelly.mae@student.umn.ac.id)";

dataStep.runCode result=r status=rc / code='/* BEGIN data step with the output table                                           data */
data "4c7208d2-f0a0-44c4-9eb9-754740090822" (caslib="CASUSER(kelly.mae@student.umn.ac.id)" promote="no");

    length
       "OrderDate"n 8
       "Shipping Date"n 8
    ;
    label
        "Shipping Date"n=""
    ;
    format
        "OrderDate"n DATE9.
        "Shipping Date"n DATE9.
    ;

    /* Set the input                                                                set */
    set "EVERZONE WAREHOUSE DATASET" (caslib="CASUSER(kelly.mae@student.umn.ac.id)"  );

    /* BEGIN statement_5b9646b5_bd31_41ab_b009_981ed56daa19              convert_column */
    "OrderDate"n= INPUT(strip("Order Date"n),ANYDTDTE10.);
    /* END statement_5b9646b5_bd31_41ab_b009_981ed56daa19                convert_column */

    /* BEGIN statement_5b9646b5_bd31_41ab_b009_981ed56daa19              convert_column */
    "Shipping Date"n= INPUT(strip("Ship Date"n),ANYDTDTE10.);
    /* END statement_5b9646b5_bd31_41ab_b009_981ed56daa19                convert_column */

/* END data step                                                                    run */
run;
';
if rc.statusCode != 0 then do;
  print "Error executing datastep";
  exit 2;
end;
  _dp_inputTable="4c7208d2-f0a0-44c4-9eb9-754740090822";
  _dp_inputCaslib="CASUSER(kelly.mae@student.umn.ac.id)";

  _dp_outputTable="4c7208d2-f0a0-44c4-9eb9-754740090822";
  _dp_outputCaslib="CASUSER(kelly.mae@student.umn.ac.id)";

dataStep.runCode result=r status=rc / code='/* BEGIN data step with the output table                                           data */
data "4c7208d2-f0a0-44c4-9eb9-754740090822" (caslib="CASUSER(kelly.mae@student.umn.ac.id)" promote="no");

    length
       "Order Date"n 8
    ;
    label
        "Order Date"n=""
    ;
    format
        "Order Date"n DATE9.
    ;

    /* Set the input                                                                set */
    set "4c7208d2-f0a0-44c4-9eb9-754740090822" (caslib="CASUSER(kelly.mae@student.umn.ac.id)"   drop="Order Date"n  drop="Ship Date"n  rename=("OrderDate"n = "Order Date"n) );

/* END data step                                                                    run */
run;
';
if rc.statusCode != 0 then do;
  print "Error executing datastep";
  exit 2;
end;
  _dp_inputTable="4c7208d2-f0a0-44c4-9eb9-754740090822";
  _dp_inputCaslib="CASUSER(kelly.mae@student.umn.ac.id)";

  _dp_outputTable="EVERZONE WAREHOUSE DATASET_NEW";
  _dp_outputCaslib="CASUSER(kelly.mae@student.umn.ac.id)";

srcCasTable="4c7208d2-f0a0-44c4-9eb9-754740090822";
srcCasLib="CASUSER(kelly.mae@student.umn.ac.id)";
tgtCasTable="EVERZONE WAREHOUSE DATASET_NEW";
tgtCasLib="CASUSER(kelly.mae@student.umn.ac.id)";
saveType="sashdat";
tgtCasTableLabel="";
replace=1;
saveToDisk=1;

exists = doesTableExist(tgtCasLib, tgtCasTable);
if (exists !=0) then do;
  if (replace == 0) then do;
    print "Table already exists and replace flag is set to false.";
    exit ({severity=2, reason=5, formatted="Table already exists and replace flag is set to false.", statusCode=9});
  end;
end;

if (saveToDisk == 1) then do;
  /* Save will automatically save as type represented by file ext */
  saveName=tgtCasTable;
  if(saveType != "") then do;
    saveName=tgtCasTable || "." || saveType;
  end;
  table.save result=r status=rc / caslib=tgtCasLib name=saveName replace=replace
    table={
      caslib=srcCasLib
      name=srcCasTable
    };
  if rc.statusCode != 0 then do;
    return rc.statusCode;
  end;
  tgtCasPath=dictionary(r, "name");

  dropTableIfExists(tgtCasLib, tgtCasTable);
  dropTableIfExists(tgtCasLib, tgtCasTable);

  table.loadtable result=r status=rc / caslib=tgtCasLib path=tgtCasPath casout={name=tgtCasTable caslib=tgtCasLib} promote=1;
  if rc.statusCode != 0 then do;
    return rc.statusCode;
  end;
end;

else do;
  dropTableIfExists(tgtCasLib, tgtCasTable);
  dropTableIfExists(tgtCasLib, tgtCasTable);
  table.promote result=r status=rc / caslib=srcCasLib name=srcCasTable target=tgtCasTable targetLib=tgtCasLib;
  if rc.statusCode != 0 then do;
    return rc.statusCode;
  end;
end;


dropTableIfExists("CASUSER(kelly.mae@student.umn.ac.id)", "4c7208d2-f0a0-44c4-9eb9-754740090822");

function doesTableExist(casLib, casTable);
  table.tableExists result=r status=rc / caslib=casLib table=casTable;
  tableExists = dictionary(r, "exists");
  return tableExists;
end func;

function dropTableIfExists(casLib,casTable);
  tableExists = doesTableExist(casLib, casTable);
  if tableExists != 0 then do;
    print "Dropping table: "||casLib||"."||casTable;
    table.dropTable result=r status=rc/ caslib=casLib table=casTable quiet=0;
    if rc.statusCode != 0 then do;
      exit();
    end;
  end;
end func;

/* Return list of columns in a table */
function columnList(casLib, casTable);
  table.columnInfo result=collist / table={caslib=casLib,name=casTable};
  ndimen=dim(collist['columninfo']);
  featurelist={};
  do i =  1 to ndimen;
    featurelist[i]=upcase(collist['columninfo'][i][1]);
  end;
  return featurelist;
end func;
