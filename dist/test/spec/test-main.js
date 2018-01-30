'use strict';

var _prunk = require('prunk');

var _prunk2 = _interopRequireDefault(_prunk);

var _jsdom = require('jsdom');

var _chai = require('chai');

var _chai2 = _interopRequireDefault(_chai);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

// Mock Grafana modules that are not available outside of the core project
// Required for loading module.js
_prunk2.default.mock('./css/query-editor.css!', 'no css, dude.');
_prunk2.default.mock('app/plugins/sdk', {
    QueryCtrl: null
});

// Setup jsdom
// Required for loading angularjs
global.document = (0, _jsdom.jsdom)('<html><head><script></script></head><body></body></html>');
global.window = global.document.parentWindow;

// Setup Chai
_chai2.default.should();
global.assert = _chai2.default.assert;
global.expect = _chai2.default.expect;
//# sourceMappingURL=test-main.js.map
