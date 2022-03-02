#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <string>

typedef enum {heap, sorted, tree} fType;
typedef enum {readMode, writeMode} Mode;

// stub DBFile header..replace it with your own DBFile.h 

class DBFile {

private:
	File file;
	string fileName;
	Page page;
	off_t pointer;
	ComparisonEngine comparisonEngine;
	Mode mode;
	fType fileType;

public:
	DBFile (); 

	// Cusotm Added
	void WriteMetaData ();
	fType ReadMetaData ();
	int CheckOverFlow ();
	void WriteMode ();
	void ReadMode ();

	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};
#endif
