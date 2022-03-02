#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
#include <fstream>

// stub file .. replace it with your own DBFile.cc


DBFile::DBFile () {
    mode = readMode;
    pointer = 0;
}

void DBFile::WriteMetaData(){
    ofstream MyFile(fileName+".meta");
    string metaData;
    switch (fileType)
    {
        case heap:
            metaData = "heap";
            break;
        case sorted:
            metaData = "sorted";
            break;
        case tree:
            metaData = "tree";
            break;
        default:
            break;
    }
    MyFile << metaData;
    MyFile.close();
}

fType DBFile::ReadMetaData(){
    string fileType;
    ifstream MyReadFile(fileName+".meta");
    getline (MyReadFile, fileType);
    MyReadFile.close();
    if(fileType.compare("heap")==0){
        return heap;
    }
    else if(fileType.compare("sorted")==0){
        return sorted;
    }
    else if(fileType.compare("tree")==0){
        return tree;
    }
    return heap;
}

int DBFile::CheckOverFlow (){
    // cout << "pointer " << pointer << ", File Length: "<< file.GetLength() << "\n";
    if(pointer < file.GetLength()-1){
        return 1;
    }
    return 0;
}

void DBFile::WriteMode(){
    if(mode == readMode){
        page.EmptyItOut();
        file.GetLastPage(&page);
    }
    mode = writeMode;
    return;
}

void DBFile::ReadMode(){
    if (mode == writeMode){
        file.AddPage(&page,pointer);
    }
    mode = readMode;
    return;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    switch (f_type)
    {
        case heap:
            file.Open(0,const_cast<char *>(f_path));
            ReadMode();
            pointer = 0;
            fileType = heap;
            fileName = f_path;
            return 1;
        case sorted:
            return 1;
        case tree:
            return 1;
        default:
            return 0;
    }
    
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    FILE *loadedTextFile = fopen(loadpath, "r");
    Record record;
    mode = writeMode;
    int counter = 0;
    while(record.SuckNextRecord(&f_schema,loadedTextFile)){
        counter++;
        Add(record);
    }
    cout << "Number of records added: " << counter << "\n";
}

int DBFile::Open (const char *f_path) { 
    fileName = f_path;
    file.Open(1,const_cast<char *>(f_path));
    pointer = 0;
    fileType = ReadMetaData();
    ReadMode();
    file.GetPage(&page,pointer);
    return 1;
}

void DBFile::MoveFirst () {
    pointer = 0;
}

int DBFile::Close () {
    // To flush everything to disk before closing
    if(mode == writeMode){
        file.AddPage(&page,pointer++);
        page.EmptyItOut();
    }

    WriteMetaData();
    int fileSize = file.Close();
    cout << "FileSize: " << fileSize << "\n";
    return 1;
}

void DBFile::Add (Record &rec) {
    WriteMode();

    Record record; 
    record.Consume(&rec);

    if(page.Append(&record) == 0){
        // cout << "Page length : " << page.GetNumRecs() << "\n";
        file.AddPage(&page,pointer++);
        page.EmptyItOut();
        page.Append(&record);
    }
}

int DBFile::GetNext (Record &fetchme) {
    ReadMode();
    if(page.GetFirst(&fetchme)==0){
        pointer++;
        if(CheckOverFlow()==0){
            return 0;
        }
        page.EmptyItOut();
        file.GetPage(&page,pointer);
        page.GetFirst(&fetchme);
    }
    return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ReadMode();
    while(GetNext(fetchme) == 1){
        if(comparisonEngine.Compare(&fetchme,&literal,&cnf)==1){
            return 1;
        }
    }
    return 0;
}
