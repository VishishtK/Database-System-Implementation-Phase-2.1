#include "gtest/gtest.h"
#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include "DBFile.cc"
#include "BigQ.h"

#include <stdlib.h>
using namespace std;

class BigQTest : public ::testing::Test {
protected:
    BigQTest() {
        // You can do set-up work for each test here.
        file =  new File();
        file->Open(1, "tmp.bin");
    }

    ~BigQTest() override {
        // You can do clean-up work that doesn't throw exceptions here.
    }
    void SetUp() override {
        // Code here will be called immediately after the constructor (right
        // before each test).
            
    }

    void TearDown() override {
        // Code here will be called immediately after each test (right
        // before the destructor).
    }

    File* file;
};

TEST_F(BigQTest, ConstructorTest) {

    int runlen = 10;
    int index = 0;
    class Run *run = new class Run(file,index,index+runlen);


    EXPECT_EQ(run->getStart(), 1);
    EXPECT_EQ(run->getEnd(), index+runlen);
}

TEST_F(BigQTest, getRecordPointer) {
    int runlen = 10;
    int index = 0;
    class Run *run = new class Run(file,index,index+runlen);
    EXPECT_EQ(typeid(*run->getRecordPointer()),typeid(Record));
}

TEST_F(BigQTest, RunOnEmptyFile) {
    
    File emptyFile;
    emptyFile.Open(0, "emptyTmp.bin");
    int runlen = 0;
    int index = 0;
    class Run *run = new class Run(file,index,index+runlen);


    EXPECT_EQ(run->getStart(), 0);
    EXPECT_EQ(run->getEnd(), index+runlen);
}

TEST_F(BigQTest, NextRecordOfRun) {
    int runlen = 10;
    int index = 0;
    class Run *run = new class Run(file,index,index+runlen);
    EXPECT_EQ(run->Next(),1);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}