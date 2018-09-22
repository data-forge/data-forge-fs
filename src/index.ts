import { assert } from 'chai';
import { IDataFrame, DataFrame, fromJSON, fromCSV, ICSVOptions } from 'data-forge-beta';
import * as dataForge from 'data-forge-beta';

/** 
 * Packages a dataframe ready for serialization to a CSV format text file.
 */
export interface ICsvSerializer {

    /**
     * Serialize the dataframe to the CSV data format and save it as a text file in the local file system.
     * Asynchronous version using the Node.js 'fs' module.
     * 
     * @param filePath Specifies the path for the output file. 
     * 
     * @return Returns a promise that resolves when the file has been written.
     * 
     * 
     * @example
     * <pre>
     * 
     * await df.asCSV().writeFile("my-data-file.csv");
     * </pre>
     */
    /*async*/ writeFile (filePath: string): Promise<void>;

    /**
     * Serialize the dataframe to the CSV data format and save it as a text file in the local file system.
     * Synchronous version using the Node.js 'fs' module.
     * 
     * @param filePath Specifies the path for the output file. 
     * 
     * @example
     * <pre>
     * 
     * df.asCSV().writeFileSync("my-data-file.csv");
     * </pre>
     */
    writeFileSync (filePath: string): void;
}

/**
 * @hidden
 * Packages a dataframe ready for serialization to a CSV format text file.
 */
class CsvSerializer<IndexT, ValueT> implements ICsvSerializer {

    dataframe: IDataFrame<IndexT, ValueT>;

    constructor (dataframe: IDataFrame<IndexT, ValueT>) {
        this.dataframe = dataframe;
    }
    
    /**
     * Serialize the dataframe to the CSV data format and save it as a text file in the local file system.
     * Asynchronous version using the Node.js 'fs' module.
     * 
     * @param filePath Specifies the path for the output file. 
     * 
     * @return Returns a promise that resolves when the file has been written.
     * 
     * 
     * @example
     * <pre>
     * 
     * await df.asCSV().writeFile("my-data-file.csv");
     * </pre>
     */
    writeFile (filePath: string): Promise<void> {
        assert.isString(filePath, "Expected 'filePath' parameter to 'DataFrame.asCSV().writeFile' to be a string that specifies the path of the file to write to the local file system.");

        return new Promise((resolve, reject) => {
            const fs = require('fs');	
            fs.writeFile(filePath, this.dataframe.toCSV(), (err: any) => {
                if (err) {
                    reject(err);
                    return;
                }

                resolve();
            });
        });
    }

    /**
     * Serialize the dataframe to the CSV data format and save it as a text file in the local file system.
     * Synchronous version using the Node.js 'fs' module.
     * 
     * @param filePath Specifies the path for the output file. 
     * 
     * @example
     * <pre>
     * 
     * df.asCSV().writeFileSync("my-data-file.csv");
     * </pre>
     */
    writeFileSync (filePath: string): void {
        assert.isString(filePath, "Expected 'filePath' parameter to 'DataFrame.asCSV().writeFileSync' to be a string that specifies the path of the file to write to the local file system.");

        const fs = require('fs');	
        fs.writeFileSync(filePath, this.dataframe.toCSV());
    }
}

/**
 * Packages a dataframe ready for serialization to a JSON format text file.
 */
export interface IJsonSerializer {

    /**
     * Serialize the dataframe to the JSON data format and save it as a text file in the local file system.
     * Asynchronous version using the Node.js 'fs' module.
     * 
     * @param filePath Specifies the path for the output file. 
     * 
     * @return Returns a promise that resolves when the file has been written.
     * 
     * 
     * @example
     * <pre>
     * 
     * await df.asJSON().writeFile("my-data-file.json");
     * </pre>
     */
    /*async*/ writeFile (filePath: string): Promise<void>;

    /**
     * Serialize the dataframe to the JSON data format and save it as a text file in the local file system.
     * Synchronous version using the Node.js 'fs' module.
     * 
     * @param filePath Specifies the path for the output file. 
     * 
     * @example
     * <pre>
     * 
     * df.asJSON().writeFileSync("my-data-file.json");
     * </pre>
     */
    writeFileSync (filePath: string): void;
}

/**
 * @hidden
 * Packages a dataframe ready for serialization to a JSON format text file.
 */
class JsonSerializer<IndexT, ValueT> implements IJsonSerializer {

    dataframe: IDataFrame<IndexT, ValueT>;

    constructor (dataframe: IDataFrame<IndexT, ValueT>) {
        this.dataframe = dataframe;
    }

    /**
     * Serialize the dataframe to the JSON data format and save it as a text file in the local file system.
     * Asynchronous version using the Node.js 'fs' module.
     * 
     * @param filePath Specifies the path for the output file. 
     * 
     * @return Returns a promise that resolves when the file has been written.
     * 
     * 
     * @example
     * <pre>
     * 
     * await df.asJSON().writeFile("my-data-file.json");
     * </pre>
     */
    writeFile (filePath: string): Promise<void> {
        assert.isString(filePath, "Expected 'filePath' parameter to 'DataFrame.asJSON().writeFile' to be a string that specifies the path of the file to write to the local file system.");

        return new Promise((resolve, reject) => {
            const fs = require('fs');	
            fs.writeFile(filePath, this.dataframe.toJSON(), (err: any) => {
                if (err) {
                    reject(err);
                    return;
                }

                resolve();
            });
        });
    }

    /**
     * Serialize the dataframe to the JSON data format and save it as a text file in the local file system.
     * Synchronous version using the Node.js 'fs' module.
     * 
     * @param filePath Specifies the path for the output file. 
     * 
     * @example
     * <pre>
     * 
     * df.asJSON().writeFileSync("my-data-file.json");
     * </pre>
     */
    writeFileSync (filePath: string): void {
        assert.isString(filePath, "Expected 'filePath' parameter to 'DataFrame.asJSON().writeFile' to be a string that specifies the path of the file to write to the local file system.");

        const fs = require('fs');	
        fs.writeFileSync(filePath, this.dataframe.toJSON());
    }
}

//
// Augment IDataFrame and DataFrame with fs functions.
//
declare module "data-forge-beta/build/lib/dataframe" {

    /**
     * Interface that represents a dataframe.
     * A dataframe contains an indexed sequence of data records.
     * Think of it as a spreadsheet or CSV file in memory.
     * 
     * Each data record contains multiple named fields, the value of each field represents one row in a column of data.
     * Each column of data is a named {@link Series}.
     * You think of a dataframe a collection of named data series.
     * 
     * @typeparam IndexT The type to use for the index.
     * @typeparam ValueT The type to use for each row/data record.
     */
    interface IDataFrame<IndexT, ValueT> {

        /**
         * Treat the dataframe as CSV data for purposes of serialization.
         * This is the first step you need in serializing a dataframe to a CSV data file.
         * 
         * @return Returns a {@link ICsvSerializer} that represents the dataframe for serialization in the CSV format. Call `writeFile` or `writeFileSync` to output the CSV data to a text file.
         * 
         * @example
         * <pre>
         * 
         * df.asCSV().writeFileSync("my-data-file.csv");
         * </pre>
         * 
         * @example
         * <pre>
         * 
         * await df.asCSV().writeFile("my-data-file.csv");
         * </pre>
         */
        asCSV(): ICsvSerializer;

        /**
         * Treat the dataframe as JSON data for purposes of serialization.
         * This is the first step you need in serializing a dataframe to a JSON data file.
         * 
         * @return Returns a {@link IJsonSerializer} that represents the dataframe for serialization in the JSON format. Call `writeFile` or `writeFileSync` to output the JSON data to a text file.
         * 
         * @example
         * <pre>
         * 
         * df.asJSON().writeFileSync("my-data-file.json");
         * </pre>
         * 
         * @example
         * <pre>
         * 
         * await df.asJSON().writeFile("my-data-file.json");
         * </pre>
         */
        asJSON(): IJsonSerializer;
    }

    /**
     * Class that represents a dataframe.
     * A dataframe contains an indexed sequence of data records.
     * Think of it as a spreadsheet or CSV file in memory.
     * 
     * Each data record contains multiple named fields, the value of each field represents one row in a column of data.
     * Each column of data is a named {@link Series}.
     * You think of a dataframe a collection of named data series.
     * 
     * @typeparam IndexT The type to use for the index.
     * @typeparam ValueT The type to use for each row/data record.
     */
    interface DataFrame<IndexT, ValueT> {

        /**
         * Treat the dataframe as CSV data for purposes of serialization.
         * This is the first step you need in serializing a dataframe to a CSV data file.
         * 
         * @return Returns a {@link ICsvSerializer} that represents the dataframe for serialization in the CSV format. Call `writeFile` or `writeFileSync` to output the CSV data to a text file.
         * 
         * @example
         * <pre>
         * 
         * df.asCSV().writeFileSync("my-data-file.csv");
         * </pre>
         * 
         * @example
         * <pre>
         * 
         * await df.asCSV().writeFile("my-data-file.csv");
         * </pre>
         */
        asCSV(): ICsvSerializer;

        /**
         * Treat the dataframe as JSON data for purposes of serialization.
         * This is the first step you need in serializing a dataframe to a JSON data file.
         * 
         * @return Returns a {@link IJsonSerializer} that represents the dataframe for serialization in the JSON format. Call `writeFile` or `writeFileSync` to output the JSON data to a text file.
         * 
         * @example
         * <pre>
         * 
         * df.asJSON().writeFileSync("my-data-file.json");
         * </pre>
         * 
         * @example
         * <pre>
         * 
         * await df.asJSON().writeFile("my-data-file.json");
         * </pre>
         */
        asJSON(): IJsonSerializer;
    }
}

/**
 * Treat the dataframe as CSV data for purposes of serialization.
 * This is the first step you need in serializing a dataframe to a CSV data file.
 * 
 * @return Returns a {@link ICsvSerializer} that represents the dataframe for serialization in the CSV format. Call `writeFile` or `writeFileSync` to output the CSV data to a text file.
 * 
 * @example
 * <pre>
 * 
 * df.asCSV().writeFileSync("my-data-file.csv");
 * </pre>
 * 
 * @example
 * <pre>
 * 
 * await df.asCSV().writeFile("my-data-file.csv");
 * </pre>
 */
export function asCSV<IndexT, ValueT>(this: IDataFrame<IndexT, ValueT>): ICsvSerializer {
    return new CsvSerializer<IndexT, ValueT>(this);
}

/**
 * Treat the dataframe as JSON data for purposes of serialization.
 * This is the first step you need in serializing a dataframe to a JSON data file.
 * 
 * @return Returns a {@link IJsonSerializer} that represents the dataframe for serialization in the JSON format. Call `writeFile` or `writeFileSync` to output the JSON data to a text file.
 * 
 * @example
 * <pre>
 * 
 * df.asJSON().writeFileSync("my-data-file.json");
 * </pre>
 * 
 * @example
 * <pre>
 * 
 * await df.asJSON().writeFile("my-data-file.json");
 * </pre>
 */
export function asJSON<IndexT, ValueT>(this: IDataFrame<IndexT, ValueT>): IJsonSerializer {
    return new JsonSerializer<IndexT, ValueT>(this);
}

DataFrame.prototype.asCSV = asCSV;
DataFrame.prototype.asJSON = asJSON;  

//
// Promise-based read file.
//
function readFileData(filePath: string): Promise<string> {
    return new Promise<string>((resolve, reject) => {
        const fs = require('fs');
        fs.readFile(filePath, 'utf8', (err: any, fileData: string) => {
            if (err) {
                reject(err);
                return;
            }
    
            resolve(fileData);
        });
    });
}

/**
 * Reads a file asynchonrously to a dataframe.
 */
export interface IAsyncFileReader {

    /**
     * Deserialize a CSV file to a DataFrame.
     * Returns a promise that later resolves to a DataFrame.
     * 
     * @param [config] Optional configuration file for parsing.
     * 
     * @returns Returns a promise of a dataframe loaded from the file. 
     */
    parseCSV (config?: ICSVOptions): Promise<IDataFrame<number, any>>;

    /**
     * Deserialize a JSON file to a DataFrame.
     * Returns a promise that later resolves to a DataFrame.
     * 
     * @returns Returns a promise of a dataframe loaded from the file. 
     */
    parseJSON (): Promise<IDataFrame<number, any>>;
}

/**
 * @hidden
 * Reads a file asynchonrously to a dataframe.
 */
class AsyncFileReader implements IAsyncFileReader {

    filePath: string;

    constructor(filePath: string) {
        this.filePath = filePath;
    }

    /**
     * Deserialize a CSV file to a DataFrame.
     * Returns a promise that later resolves to a DataFrame.
     * 
     * @param [config] Optional configuration file for parsing.
     * 
     * @returns Returns a promise of a dataframe loaded from the file. 
     */
    async parseCSV (config?: ICSVOptions): Promise<IDataFrame<number, any>> {
        if (config) {
            assert.isObject(config, "Expected optional 'config' parameter to dataForge.readFile(...).parseCSV(...) to be an object with configuration options for CSV parsing.");
        }
        
        const fileData = await readFileData(this.filePath);
        return fromCSV(fileData, config);
    }

    /**
     * Deserialize a JSON file to a DataFrame.
     * Returns a promise that later resolves to a DataFrame.
     * 
     * @returns Returns a promise of a dataframe loaded from the file. 
     */
    async parseJSON (): Promise<IDataFrame<number, any>> {
        const fileData = await readFileData(this.filePath);
        return fromJSON(fileData);
    } 
}

/**
 * Reads a file synchonrously to a dataframe.
 */
export interface ISyncFileReader {

    /**
     * Deserialize a CSV file to a DataFrame.
     * 
     * @param [config] Optional configuration file for parsing.
     * 
     * @returns Returns a dataframe that was deserialized from the file.
     */
    parseCSV (config?: ICSVOptions): IDataFrame<number, any>;

    /**
     * Deserialize a JSON file to a DataFrame.
     * 
     * @returns {DataFrame} Returns a dataframe that was deserialized from the file.  
     */
    parseJSON (): IDataFrame<number, any>;
}

/**
 * @hidden
 * Reads a file synchonrously to a dataframe.
 */
class SyncFileReader implements ISyncFileReader {

    filePath: string;

    constructor(filePath: string) {
        this.filePath = filePath;
    }

    /**
     * Deserialize a CSV file to a DataFrame.
     * 
     * @param [config] Optional configuration file for parsing.
     * 
     * @returns Returns a dataframe that was deserialized from the file.
     */
    parseCSV (config?: ICSVOptions): IDataFrame<number, any> {
        if (config) {
            assert.isObject(config, "Expected optional 'config' parameter to dataForge.readFileSync(...).parseCSV(...) to be an object with configuration options for CSV parsing.");
        }

        const fs = require('fs');
        return fromCSV(fs.readFileSync(this.filePath, 'utf8'), config);
    }

    /**
     * Deserialize a JSON file to a DataFrame.
     * 
     * @param [config] Optional configuration file for parsing.
     * 
     * @returns Returns a dataframe that was deserialized from the file.  
     */
    parseJSON (): IDataFrame<number, any> {

        const fs = require('fs');
        return fromJSON(fs.readFileSync(this.filePath, 'utf8'));
    } 
}

//
// Augmuent the data-forge namespace to add new functions.
//
declare module "data-forge-beta" {

    /**
     * Read a file asynchronously from the file system.
     * Works in Nodejs, doesn't work in the browser.
     * 
     * @param filePath The path to the file to read.
     * 
     * @returns Returns an object that represents the file. Use `parseCSV` or `parseJSON` to deserialize to a DataFrame.
     */
    function readFile (filePath: string): IAsyncFileReader;

    /**
     * Read a file synchronously from the file system.
     * Works in Nodejs, doesn't work in the browser.
     * 
     * @param filePath The path to the file to read.
     * 
     * @returns Returns an object that represents the file. Use `parseCSV` or `parseJSON` to deserialize to a DataFrame.
     */
    export function readFileSync (filePath: string): ISyncFileReader    
}

/**
 * Read a file asynchronously from the file system.
 * Works in Nodejs, doesn't work in the browser.
 * 
 * @param filePath The path to the file to read.
 * 
 * @returns Returns an object that represents the file. Use `parseCSV` or `parseJSON` to deserialize to a DataFrame.
 */
export function readFile (filePath: string): IAsyncFileReader {

    assert.isString(filePath, "Expected 'filePath' parameter to dataForge.readFile to be a string that specifies the path of the file to read.");

    return new AsyncFileReader(filePath);
}

/**
 * Read a file synchronously from the file system.
 * Works in Nodejs, doesn't work in the browser.
 * 
 * @param filePath The path to the file to read.
 * 
 * @returns Returns an object that represents the file. Use `parseCSV` or `parseJSON` to deserialize to a DataFrame.
 * 
 * @memberOf Data-Forge
 */
export function readFileSync (filePath: string): ISyncFileReader {

    assert.isString(filePath, "Expected 'filePath' parameter to dataForge.readFileSync to be a string that specifies the path of the file to read.");

    return new SyncFileReader(filePath);
}

//
// Patch in the plugin module.
// This feels a bit dodgey, but it works!
// 
(dataForge as any).readFile = readFile;
(dataForge as any).readFileSync = readFileSync;
