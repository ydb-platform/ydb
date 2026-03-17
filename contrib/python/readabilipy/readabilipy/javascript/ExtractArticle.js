/*
 * This file is part of ReadabiliPy
 */

const fs = require('fs');
const { Readability } = require('@mozilla/readability');
const { JSDOM } = require('jsdom');

function readFile(filePath) {
	return fs.readFileSync(filePath, {encoding: "utf-8"}).trim();
}

function writeFile(data, filePath) {
	return fs.writeFileSync(filePath, data, {encoding: "utf-8"});
}
function main() {
	var outFilePath;

	var argv = require('minimist')(process.argv.slice(2));
	if (argv['i'] === undefined) {
		console.log("Input file required.");
		return 1;
	}

	var inFilePath = argv['i'];
	if (typeof(argv['o']) !== 'undefined') {
		outFilePath = argv['o'];
	} else {
		outFilePath = inFilePath + ".simple.json";
	}

	var html = readFile(inFilePath);
	var doc = new JSDOM(html);
	let reader = new Readability(doc.window.document);
	let article = reader.parse();

	writeFile(JSON.stringify(article), outFilePath);
	return 0;
}

main();
