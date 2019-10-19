console.log('Loading function');
exports.handler = async (event, context) => {
    /* Process the list of records and transform them */
    const process = event.records.map((record) => {
        let dataFormatted = JSON.parse((Buffer.from(record.data, 'base64')));
        let response = {
            recordId: record.recordId,
            result: 'Ok',
            data:this.transformData(dataFormatted)
        }
        return response;
    });
    console.log(`Processing completed.  Successful records ${process.length}.`);
    return { records: process };
};

exports.transformData = (data) => {
    var node = this.mapper(data);
    console.log(node);
    var reducerOutCome = this.reduce(node);
    return reducerOutCome;
};

exports.mapper = (data) => {
    console.log(data.projectCode);
    var nodeStruct = {
        id: data._id,
        projectCode: data.projectCode
    }
    return nodeStruct;
};

exports.reduce = (node) => {
    const neo4jUrl = 'bolt://db-xvwxuy811zirtlf14lmz.graphenedb.com:24787';
    const neo4j = require('neo4j-driver').v1;
    console.log(neo4j);
    const neoDriver = neo4j.driver(neo4jUrl, neo4j.auth.basic('dbopsuser', 'b.G6oUmNKKs9MD.3Gxn1WfHSoUw0OGa'));
    const neoSession = neoDriver.session();
    console.log(neoSession)
    neoSession.run(
        'MERGE(n:Project {id: $id, projectCode: $projectCode}) RETURN n',
        {id: node.id, projectCode: node.projectCode}
    ).subscribe({
        onKeys: keys => {
          console.log(keys);
        },
        onNext: record => {
          console.log(record);
        },
        onCompleted: () => {
          neoSession.close();
          neoDriver.close();
        },
        onError: error => {
          console.log(error);
          neoDriver.close();
        }
      });
    console.log('ResponseObject: ', node);
    return node;
};