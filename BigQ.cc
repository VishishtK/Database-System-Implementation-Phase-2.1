#include <algorithm>
#include <queue>
#include "BigQ.h"

// TODO: Make function for adding run in tppmsSort

Run::Run(File* filePtr, int start, int end){
	this->filePtr = filePtr;
	this->start = start;
	this->end = end;
	filePtr->GetPage(&bufferPage,this->start++);
	bufferPage.GetFirst(&record);
}

Record* Run::getRecordPointer(){
	return &record;
}

int Run::getStart(){
	return start;
}

int Run::getEnd(){
	return end;
}

int Run::Next(){
	if(bufferPage.GetFirst(&record)==0){
		cout<<"Start: " << start << " End: " << end << "\n";
		if(start>=end){
			return 0;
		}
		filePtr->GetPage(&bufferPage,start++);
		bufferPage.GetFirst(&record);
	}
	return 1;
}

void* tppms(void* args){
	BigQ *bigQ = (BigQ*)args;
	cout<<"Sort starting\n";
	bigQ->tppmsSort();
	cout<<"Run Creation starting\n";
	bigQ->createRuns();
	cout<<"Merge starting\n";
	bigQ->tppmsMerge();
	return nullptr;	
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	this->in = &in;
	this->out = &out;
	this->sortorder = &sortorder;
	this->runlen = runlen;

	file.Open(0, "tmp.bin");

	pthread_t worker;
	pthread_create(&worker, NULL, tppms, (void*)this);

    // finally shut down the out pipe
	pthread_join(worker,NULL);
	out.ShutDown ();
}

BigQ::~BigQ () {
	// Delete
}

void BigQ::sortRunAndWriteToFile(vector<Record*> &recordsForSorting,off_t &pagePointerInFile,int &currRunSize, int &recordCounter){
	sort(
		recordsForSorting.begin(),
		recordsForSorting.end(),
		[this](Record* left, Record* right){
			return comparisonEngine.Compare(left,right,sortorder)<0;
		}
	);
	Page bufferPage;
	for(Record *rec : recordsForSorting){
		if(bufferPage.Append(rec)==0){
			file.AddPage(&bufferPage,pagePointerInFile++);
			bufferPage.EmptyItOut();
			bufferPage.Append(rec);
		}
		recordCounter++;
	}
	
	if(!bufferPage.isEmpty()){
		file.AddPage(&bufferPage,pagePointerInFile++);
		bufferPage.EmptyItOut();
	}
	currRunSize = 0;
	recordsForSorting.clear();
}

// read data from in pipe sort them into runlen pages
void BigQ::tppmsSort(){
	Record inputRecord;
	vector<Record*> recordsForSorting;
	off_t pagePointerInFile = 0;
	int currRunSize = 0;
	Schema lineitem ("catalog", "lineitem");
	int recordCounter = 0;
	while (this->in->Remove(&inputRecord)==1)
	{
		Record* tempRecord = new Record();
		tempRecord->Consume(&inputRecord);

		if(currRunSize+tempRecord->GetSize() > runlen * PAGE_SIZE){
			this->sortRunAndWriteToFile(recordsForSorting,pagePointerInFile,currRunSize,recordCounter);
		}
		recordsForSorting.push_back(tempRecord);
		currRunSize+=tempRecord->GetSize();
	}
	if(!recordsForSorting.empty()){
		cout<<"Adding last page\n";
		this->sortRunAndWriteToFile(recordsForSorting,pagePointerInFile,currRunSize,recordCounter);
	}

	cout<<"Sorted " << recordCounter <<" records into chunks\n";
	cout<<"temp file size " << file.GetLength()<<"\n";
}

// bool BigQ::compare(Run* left, Run* right){
// 	return comparisonEngine.Compare(left->getRecordPointer(),right->getRecordPointer(),sortorder)>=0;
// }

void BigQ::createRuns(){
	int index = 0;

	while(index+runlen < file.GetLength()){
		runs.push_back(new Run(&file,index,index+runlen));
		index = index + runlen + 1;
	}
	runs.push_back(new Run(&file,index,file.GetLength()-1));

	// for(Run* run:runs){
	// 	cout<<"Run start: "<<run->getStart()<<" end: "<<run->getEnd()<<"\n";
	// }
}

// construct priority queue over sorted runs and dump sorted data into the out pipe
void BigQ::tppmsMerge(){
	auto compare = [this](Run* left, Run* right){
		return comparisonEngine.Compare(left->getRecordPointer(),right->getRecordPointer(),sortorder)>=0;
	};

	priority_queue<Run*, vector<Run*>, decltype(compare)> PQ(compare);

	for(Run* run: runs){
		// cout << "Run start" << run->getStart() <<"\n";
		PQ.push(run);
	}
    Run *run;
	while (!PQ.empty()) {
        run = PQ.top();
		out->Insert(run->getRecordPointer());
		PQ.pop();
        if (run->Next()) {
            PQ.push(run);
        }        
    }
}
