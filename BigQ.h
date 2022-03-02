#ifndef BIGQ_H
#define BIGQ_H
#include <algorithm>
#include <queue>
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;


class Run {

private:
	int start;
	int end;
	Page bufferPage;
	Record record;
	File * filePtr;

public:
	Run (File* filePtr, int start, int end);
	Record* getRecordPointer();
	int getStart();
	int getEnd();
	int Next();
};

// class PQcompare{
// 	OrderMaker *sortorder;
// 	ComparisonEngine *comparisonEngine;

// 	PQcompare(OrderMaker *sortorder, ComparisonEngine *comparisonEngine){
// 			this->sortorder = sortorder;
// 			this->comparisonEngine = comparisonEngine;
// 	};

// 	bool operator()(Run *left,Run *right) // overloading both operators 
// 	{
// 		return comparisonEngine->Compare(left->getRecordPointer(),right->getRecordPointer(),sortorder)>=0;
// 	}
// };

class BigQ {

private:
	Pipe *in;
	Pipe *out;
	OrderMaker *sortorder;
	int runlen;

	File file;
	ComparisonEngine comparisonEngine;
	
public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
	void sortRunAndWriteToFile(vector<Record*> &recordsForSorting,off_t &pagePointerInFile,int &currRunSize, int &recordCounter);
	void tppmsSort();
	void createRuns();
	void tppmsMerge();
	vector<Run*> runs;
};

#endif
