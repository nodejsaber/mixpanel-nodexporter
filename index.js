const crypto = require('crypto')
const needle = require('needle')


function MixpanelExport(opts) {
  this.opts = opts
  if (!this.opts.api_secret) {
    throw new Error('Error: api_secret must be passed to MixpanelExport constructor.')
  }
  this.api_key = this.opts.api_key
  this.api_secret = this.opts.api_secret
  this.timeout_after = this.opts.timeout_after || 10

  needle.defaults({
    open_timeout: this.opts.open_timeout || 10000,
    read_timeout: this.opts.read_timeout || 0,
  })
}


MixpanelExport.prototype.export = parameters => this.get('export', parameters)


MixpanelExport.prototype.exportStream = (parameters) => {
  const reqOpts = Object.assign(this.api_key ? {} : { username: this.api_secret }, {
    compressed: true,
    parse: true,
  })
  return needle.get(this._buildRequestURL('export', parameters), reqOpts)
}


MixpanelExport.prototype.engage = function _engage(parameters) {
  return this.get(['engage'], parameters)
}


MixpanelExport.prototype.annotations = function _annotations(parameters) {
  return this.get('annotations', parameters)
}


MixpanelExport.prototype.createAnnotation = function _createAnnotation(parameters) {
  return this.get(['annotations', 'create'], parameters)
}


MixpanelExport.prototype.updateAnnotation = function _updateAnnotation(parameters) {
  return this.get(['annotations', 'update'], parameters)
}


MixpanelExport.prototype.events = function _events(parameters) {
  return this.get('events', parameters)
}


MixpanelExport.prototype.topEvents = function _topEvents(parameters) {
  return this.get(['events', 'top'], parameters)
}


MixpanelExport.prototype.eventNames = function _eventNames(parameters) {
  return this.get(['events', 'names'], parameters)
}


MixpanelExport.prototype.eventProperties = function _eventProperties(parameters) {
  return this.get(['events', 'properties'], parameters)
}


MixpanelExport.prototype.topEventProperties = function _topEventProperties(parameters) {
  return this.get(['events', 'properties', 'top'], parameters)
}


MixpanelExport.prototype.eventPropertyValues = function _eventPropertyValues(parameters) {
  return this.get(['events', 'properties', 'values'], parameters)
}


MixpanelExport.prototype.funnels = function _funnels(parameters) {
  return this.get(['funnels'], parameters)
}


MixpanelExport.prototype.listFunnels = function _listFunnels(parameters) {
  return this.get(['funnels', 'list'], parameters)
}


MixpanelExport.prototype.segmentation = function _segmentation(parameters) {
  return this.get(['segmentation'], parameters)
}


MixpanelExport.prototype.numericSegmentation = function _numericSegmentation(parameters) {
  return this.get(['segmentation', 'numeric'], parameters)
}


MixpanelExport.prototype.sumSegmentation = function _sumSegmentation(parameters) {
  return this.get(['segmentation', 'sum'], parameters)
}


MixpanelExport.prototype.averageSegmentation = function _averageSegmentation(parameters) {
  return this.get(['segmentation', 'average'], parameters)
}


MixpanelExport.prototype.retention = function _retention(parameters) {
  return this.get(['retention'], parameters)
}


MixpanelExport.prototype.addiction = function _addiction(parameters) {
  return this.get(['retention', 'addiction'], parameters)
}


MixpanelExport.prototype.get = function _get(method, parameters) {
  const reqOpts = this.api_key ? { headers: { Authorization: `${this.api_key} ${this.api_secret}` } } : {}
  return needle('get', this._buildRequestURL(method, parameters), undefined, reqOpts)
    .then(response => this._parseResponse(method, parameters, response.body))
}


// Parses Mixpanel's strange formatting for the export endpoint.
MixpanelExport.prototype._parseResponse = function ___parseResponse(method, parameters, result) {
  if (parameters && parameters.format === 'csv') {
    return result
  }

  if (typeof result === 'object') {
    return result
  }

  if (method === 'export') {
    const step1 = result.replace(new RegExp('\\n', 'g'), ',')
    const step2 = `[${step1}]`
    const res = step2.replace(',]', ']')
    return JSON.parse(res)
  }

  return JSON.parse(result)
}


MixpanelExport.prototype._buildRequestURL = function __buildRequestURL(method, parameters) {
  const apiStub = (method === 'export') ? 'https://data.mixpanel.com/api/2.0/' : 'https://mixpanel.com/api/2.0/'
  return `${apiStub}${typeof method.join === 'function' ? method.join('/') : ''}/?${this._requestParameterString(parameters)}`
}


MixpanelExport.prototype._requestParameterString = function __requestParameterString(args) {
  const connectionParams = Object.assign({}, args)
  if (this.api_key) {
    // connectionParams.api_key = this.api_key
    connectionParams.expire = this._expireAt()
  }
  const keys = Object.keys(connectionParams).sort()

  // calculate sig only for deprecated key+secret auth
  let sig = ''
  if (this.api_key) {
    const sigKeys = keys.filter(key => key !== 'callback')
    sig = `&sig=${this._getSignature(sigKeys, connectionParams)}`
  }

  return this._getParameterString(keys, connectionParams) + sig
}

MixpanelExport.prototype._getParameterString = function __getParameterString(keys, connectionParams) {
  return keys.map(key => `${key}=${this._urlEncode(connectionParams[key])}`).join('&')
}

MixpanelExport.prototype._getSignature = function __getSignature(keys, connectionParams) {
  const sig = keys.map(key => `${key}=${this._stringifyIfArray(connectionParams[key])}`).join('') + this.api_secret

  return crypto.createHash('md5').update(sig).digest('hex')
}

MixpanelExport.prototype._urlEncode = function __urlEncode(param) {
  return encodeURIComponent(this._stringifyIfArray(param))
}

MixpanelExport.prototype._stringifyIfArray = function __stringifyIfArray(array) {
  if (!Array.isArray(array)) {
    return array
  }

  return JSON.stringify(array)
}

MixpanelExport.prototype._expireAt = function __expireAt() {
  return Math.round(new Date().getTime() / 1000) + this.timeout_after
}

module.exports = MixpanelExport
