<details>

  <summary><code>JSON</code></summary>

  ```json
{
  "annotations": {
    "list": [
      {
        "builtIn": 1,
        "datasource": {
          "type": "grafana",
          "uid": "-- Grafana --"
        },
        "enable": true,
        "hide": true,
        "iconColor": "rgba(0, 211, 255, 1)",
        "name": "Annotations & Alerts",
        "target": {
          "limit": 100,
          "matchAny": false,
          "tags": [],
          "type": "dashboard"
        },
        "type": "dashboard"
      }
    ]
  },
  "editable": true,
  "fiscalYearStartMonth": 0,
  "graphTooltip": 0,
  "id": 2,
  "links": [],
  "liveNow": false,
  "panels": [
    {
      "datasource": {
        "type": "prometheus",
        "uid": "wAz-q1M4z"
      },
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "hideFrom": {
              "legend": false,
              "tooltip": false,
              "viz": false
            }
          },
          "mappings": [
            {
              "options": {
                "1": {
                  "color": "green",
                  "index": 4,
                  "text": "Good"
                },
                "2": {
                  "color": "blue",
                  "index": 3,
                  "text": "comes to a state of equilibrium"
                },
                "3": {
                  "color": "yellow",
                  "index": 2,
                  "text": "it is worth paying attention to"
                },
                "4": {
                  "color": "orange",
                  "index": 1,
                  "text": "one step to out of service"
                },
                "5": {
                  "color": "dark-red",
                  "index": 0,
                  "text": "out of service"
                }
              },
              "type": "value"
            }
          ]
        },
        "overrides": []
      },
      "gridPos": {
        "h": 8,
        "w": 24,
        "x": 0,
        "y": 0
      },
      "id": 4,
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "pieType": "pie",
        "reduceOptions": {
          "calcs": [
            "lastNotNull"
          ],
          "fields": "",
          "values": false
        },
        "tooltip": {
          "mode": "single",
          "sort": "none"
        }
      },
      "pluginVersion": "9.1.3",
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "wAz-q1M4z"
          },
          "editorMode": "code",
          "expr": "(vector(5) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"RED\",TYPE=~\"STORAGE_GROUP|COMPUTE\",MESSAGE!=\"Compute is overloaded\"} > 0)) or\n(vector(4) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"ORANGE\",TYPE=~\"STORAGE_GROUP|COMPUTE\",MESSAGE!=\"Compute is overloaded\"} > 0)) or\n(vector(3) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"YELLOW\",TYPE=~\"STORAGE_GROUP|COMPUTE\",MESSAGE!=\"Compute is overloaded\"} > 0)) or\n(vector(2) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"BLUE\",TYPE=~\"STORAGE_GROUP|COMPUTE\",MESSAGE!=\"Compute is overloaded\"} > 0)) or vector(1) ",
          "legendFormat": "State",
          "range": true,
          "refId": "A"
        }
      ],
      "title": "Status",
      "type": "piechart"
    },
    {
      "datasource": {
        "type": "prometheus",
        "uid": "wAz-q1M4z"
      },
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "hideFrom": {
              "legend": false,
              "tooltip": false,
              "viz": false
            }
          },
          "mappings": [
            {
              "options": {
                "1": {
                  "color": "green",
                  "index": 4,
                  "text": "Good"
                },
                "2": {
                  "color": "blue",
                  "index": 3,
                  "text": "comes to a state of equilibrium"
                },
                "3": {
                  "color": "yellow",
                  "index": 2,
                  "text": "it is worth paying attention to"
                },
                "4": {
                  "color": "orange",
                  "index": 1,
                  "text": "one step to out of service"
                },
                "5": {
                  "color": "dark-red",
                  "index": 0,
                  "text": "out of service"
                }
              },
              "type": "value"
            }
          ]
        },
        "overrides": []
      },
      "gridPos": {
        "h": 9,
        "w": 8,
        "x": 0,
        "y": 8
      },
      "id": 5,
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "pieType": "pie",
        "reduceOptions": {
          "calcs": [
            "lastNotNull"
          ],
          "fields": "",
          "values": false
        },
        "tooltip": {
          "mode": "single",
          "sort": "none"
        }
      },
      "pluginVersion": "9.1.3",
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "wAz-q1M4z"
          },
          "editorMode": "code",
          "expr": "(vector(5) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"RED\",TYPE=\"STORAGE\", MESSAGE!~\"Storage usage over 90%\"} > 0)) or\n(vector(4) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"ORANGE\",TYPE=\"STORAGE\", MESSAGE!~\"Storage usage over 85%\"} > 0)) or\n(vector(3) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"YELLOW\",TYPE=\"STORAGE\", MESSAGE!~\"Storage usage over 75%\"} > 0)) or\n(vector(2) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"BLUE\",TYPE=\"STORAGE\", MESSAGE!~\"Storage usage\"} > 0)) or vector(1) ",
          "legendFormat": "State",
          "range": true,
          "refId": "A"
        }
      ],
      "title": "Storage",
      "type": "piechart"
    },
    {
      "datasource": {
        "type": "prometheus",
        "uid": "wAz-q1M4z"
      },
      "description": "",
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "hideFrom": {
              "legend": false,
              "tooltip": false,
              "viz": false
            }
          },
          "mappings": [
            {
              "options": {
                "1": {
                  "color": "green",
                  "index": 4,
                  "text": "Good"
                },
                "2": {
                  "color": "blue",
                  "index": 3,
                  "text": "comes to a state of equilibrium"
                },
                "3": {
                  "color": "yellow",
                  "index": 2,
                  "text": "it is worth paying attention to"
                },
                "4": {
                  "color": "orange",
                  "index": 1,
                  "text": "one step to out of service"
                },
                "5": {
                  "color": "dark-red",
                  "index": 0,
                  "text": "out of service"
                }
              },
              "type": "value"
            }
          ]
        },
        "overrides": []
      },
      "gridPos": {
        "h": 9,
        "w": 8,
        "x": 8,
        "y": 8
      },
      "id": 6,
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "pieType": "pie",
        "reduceOptions": {
          "calcs": [
            "lastNotNull"
          ],
          "fields": "",
          "values": false
        },
        "tooltip": {
          "mode": "single",
          "sort": "none"
        }
      },
      "pluginVersion": "9.1.3",
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "wAz-q1M4z"
          },
          "editorMode": "code",
          "expr": "(vector(5) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"RED\",TYPE=~\"COMPUTE\",MESSAGE!=\"Compute is overloaded\"} > 0)) or\n(vector(4) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"ORANGE\",TYPE=~\"COMPUTE\",MESSAGE!=\"Compute is overloaded\"} > 0)) or\n(vector(3) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"YELLOW\",TYPE=~\"COMPUTE\",MESSAGE!=\"Compute is overloaded\"} > 0)) or\n(vector(2) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"BLUE\",TYPE=~\"COMPUTE\",MESSAGE!=\"Compute is overloaded\"} > 0)) or vector(1) ",
          "legendFormat": "State",
          "range": true,
          "refId": "A"
        }
      ],
      "title": "Compute",
      "type": "piechart"
    },
    {
      "datasource": {
        "type": "prometheus",
        "uid": "wAz-q1M4z"
      },
      "description": "",
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "hideFrom": {
              "legend": false,
              "tooltip": false,
              "viz": false
            }
          },
          "mappings": [
            {
              "options": {
                "1": {
                  "color": "green",
                  "index": 4,
                  "text": "Good"
                },
                "2": {
                  "color": "blue",
                  "index": 3,
                  "text": "comes to a state of equilibrium"
                },
                "3": {
                  "color": "yellow",
                  "index": 2,
                  "text": "it is worth paying attention to"
                },
                "4": {
                  "color": "orange",
                  "index": 1,
                  "text": "one step to out of service"
                },
                "5": {
                  "color": "dark-red",
                  "index": 0,
                  "text": "out of service"
                }
              },
              "type": "value"
            }
          ]
        },
        "overrides": []
      },
      "gridPos": {
        "h": 9,
        "w": 8,
        "x": 16,
        "y": 8
      },
      "id": 7,
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "pieType": "pie",
        "reduceOptions": {
          "calcs": [
            "lastNotNull"
          ],
          "fields": "",
          "values": false
        },
        "tooltip": {
          "mode": "single",
          "sort": "none"
        }
      },
      "pluginVersion": "9.1.3",
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "wAz-q1M4z"
          },
          "editorMode": "code",
          "expr": "(vector(5) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"RED\",TYPE=~\".*TABLET.*\"} > 0)) or\n(vector(4) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"ORANGE\",TYPE=~\".*TABLET.*\"} > 0)) or\n(vector(3) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"YELLOW\",TYPE=~\".*TABLET.*\"} > 0)) or\n(vector(2) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"BLUE\",TYPE=~\".*TABLET.*\"} > 0)) or vector(1) ",
          "legendFormat": "State",
          "range": true,
          "refId": "A"
        }
      ],
      "title": "Tablets",
      "type": "piechart"
    },
    {
      "datasource": {
        "type": "prometheus",
        "uid": "wAz-q1M4z"
      },
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "axisCenteredZero": false,
            "axisColorMode": "text",
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": "line",
            "fillOpacity": 0,
            "gradientMode": "none",
            "hideFrom": {
              "legend": false,
              "tooltip": false,
              "viz": false
            },
            "lineInterpolation": "linear",
            "lineWidth": 1,
            "pointSize": 5,
            "scaleDistribution": {
              "type": "linear"
            },
            "showPoints": "auto",
            "spanNulls": false,
            "stacking": {
              "group": "A",
              "mode": "none"
            },
            "thresholdsStyle": {
              "mode": "off"
            }
          },
          "mappings": [],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green",
                "value": null
              },
              {
                "color": "red",
                "value": 80
              }
            ]
          }
        },
        "overrides": []
      },
      "gridPos": {
        "h": 7,
        "w": 8,
        "x": 0,
        "y": 17
      },
      "id": 8,
      "options": {
        "legend": {
          "calcs": [],
          "displayMode": "list",
          "placement": "right",
          "showLegend": true
        },
        "tooltip": {
          "mode": "single",
          "sort": "none"
        }
      },
      "pluginVersion": "9.1.3",
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "wAz-q1M4z"
          },
          "editorMode": "code",
          "expr": "sum by (TYPE, MESSAGE) (ydb_healthcheck{DOMAIN=\"$Domain\",TYPE=\"STORAGE\"}) > 0 or on() vector(0)",
          "legendFormat": "{{TYPE}} ({{MESSAGE}})",
          "range": true,
          "refId": "A"
        }
      ],
      "title": "State (Storage)",
      "type": "timeseries"
    },
    {
      "datasource": {
        "type": "prometheus",
        "uid": "wAz-q1M4z"
      },
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "axisCenteredZero": false,
            "axisColorMode": "text",
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": "line",
            "fillOpacity": 0,
            "gradientMode": "none",
            "hideFrom": {
              "legend": false,
              "tooltip": false,
              "viz": false
            },
            "lineInterpolation": "linear",
            "lineWidth": 1,
            "pointSize": 5,
            "scaleDistribution": {
              "type": "linear"
            },
            "showPoints": "auto",
            "spanNulls": false,
            "stacking": {
              "group": "A",
              "mode": "none"
            },
            "thresholdsStyle": {
              "mode": "off"
            }
          },
          "mappings": [],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green",
                "value": null
              },
              {
                "color": "red",
                "value": 80
              }
            ]
          }
        },
        "overrides": []
      },
      "gridPos": {
        "h": 7,
        "w": 8,
        "x": 8,
        "y": 17
      },
      "id": 14,
      "options": {
        "legend": {
          "calcs": [],
          "displayMode": "list",
          "placement": "right",
          "showLegend": true
        },
        "tooltip": {
          "mode": "single",
          "sort": "none"
        }
      },
      "pluginVersion": "9.1.3",
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "wAz-q1M4z"
          },
          "editorMode": "code",
          "expr": "sum by (TYPE, MESSAGE) (ydb_healthcheck{DOMAIN=\"$Domain\",TYPE=\"COMPUTE\"}) > 0 or on() vector(0)",
          "legendFormat": "{{TYPE}} ({{MESSAGE}})",
          "range": true,
          "refId": "A"
        }
      ],
      "title": "State (Compute)",
      "type": "timeseries"
    },
    {
      "datasource": {
        "type": "prometheus",
        "uid": "wAz-q1M4z"
      },
      "description": "",
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "axisCenteredZero": false,
            "axisColorMode": "text",
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": "line",
            "fillOpacity": 0,
            "gradientMode": "none",
            "hideFrom": {
              "legend": false,
              "tooltip": false,
              "viz": false
            },
            "lineInterpolation": "linear",
            "lineWidth": 1,
            "pointSize": 5,
            "scaleDistribution": {
              "type": "linear"
            },
            "showPoints": "auto",
            "spanNulls": false,
            "stacking": {
              "group": "A",
              "mode": "none"
            },
            "thresholdsStyle": {
              "mode": "off"
            }
          },
          "mappings": [],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green",
                "value": null
              },
              {
                "color": "red",
                "value": 80
              }
            ]
          }
        },
        "overrides": [
          {
            "__systemRef": "hideSeriesFrom",
            "matcher": {
              "id": "byNames",
              "options": {
                "mode": "exclude",
                "names": [
                  "SYSTEM_TABLET (System tablet response time is over 1000ms)"
                ],
                "prefix": "All except:",
                "readOnly": true
              }
            },
            "properties": [
              {
                "id": "custom.hideFrom",
                "value": {
                  "legend": false,
                  "tooltip": false,
                  "viz": true
                }
              }
            ]
          }
        ]
      },
      "gridPos": {
        "h": 7,
        "w": 8,
        "x": 16,
        "y": 17
      },
      "id": 10,
      "options": {
        "legend": {
          "calcs": [],
          "displayMode": "list",
          "placement": "right",
          "showLegend": true
        },
        "tooltip": {
          "mode": "single",
          "sort": "none"
        }
      },
      "pluginVersion": "9.1.3",
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "wAz-q1M4z"
          },
          "editorMode": "code",
          "expr": "sum by (TYPE, MESSAGE) (ydb_healthcheck{DOMAIN=\"$Domain\",TYPE=~\".*TABLET.*\"}) > 0 or on() vector(0)",
          "legendFormat": "{{TYPE}} ({{MESSAGE}})",
          "range": true,
          "refId": "A"
        }
      ],
      "title": "State (Tablets)",
      "type": "timeseries"
    },
    {
      "datasource": {
        "type": "prometheus",
        "uid": "wAz-q1M4z"
      },
      "description": "",
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "hideFrom": {
              "legend": false,
              "tooltip": false,
              "viz": false
            }
          },
          "mappings": [
            {
              "options": {
                "1": {
                  "color": "green",
                  "index": 4,
                  "text": "Good"
                },
                "2": {
                  "color": "blue",
                  "index": 3,
                  "text": "comes to a state of equilibrium"
                },
                "3": {
                  "color": "yellow",
                  "index": 2,
                  "text": "it is worth paying attention to"
                },
                "4": {
                  "color": "orange",
                  "index": 1,
                  "text": "one step to out of service"
                },
                "5": {
                  "color": "dark-red",
                  "index": 0,
                  "text": "out of service"
                }
              },
              "type": "value"
            }
          ]
        },
        "overrides": []
      },
      "gridPos": {
        "h": 9,
        "w": 8,
        "x": 0,
        "y": 24
      },
      "id": 11,
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "pieType": "pie",
        "reduceOptions": {
          "calcs": [
            "lastNotNull"
          ],
          "fields": "",
          "values": false
        },
        "tooltip": {
          "mode": "single",
          "sort": "none"
        }
      },
      "pluginVersion": "9.1.3",
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "wAz-q1M4z"
          },
          "editorMode": "code",
          "expr": "(vector(5) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"RED\",TYPE=\"PDISK\"} > 0)) or\n(vector(4) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"ORANGE\",TYPE=\"PDISK\"} > 0)) or\n(vector(3) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"YELLOW\",TYPE=\"PDISK\"} > 0)) or\n(vector(2) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"BLUE\",TYPE=\"PDISK\"} > 0)) or vector(1) ",
          "legendFormat": "State",
          "range": true,
          "refId": "A"
        }
      ],
      "title": "Pdisks",
      "type": "piechart"
    },
    {
      "datasource": {
        "type": "prometheus",
        "uid": "wAz-q1M4z"
      },
      "description": "",
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "hideFrom": {
              "legend": false,
              "tooltip": false,
              "viz": false
            }
          },
          "mappings": [
            {
              "options": {
                "1": {
                  "color": "green",
                  "index": 4,
                  "text": "Good"
                },
                "2": {
                  "color": "blue",
                  "index": 3,
                  "text": "comes to a state of equilibrium"
                },
                "3": {
                  "color": "yellow",
                  "index": 2,
                  "text": "it is worth paying attention to"
                },
                "4": {
                  "color": "orange",
                  "index": 1,
                  "text": "one step to out of service"
                },
                "5": {
                  "color": "dark-red",
                  "index": 0,
                  "text": "out of service"
                }
              },
              "type": "value"
            }
          ]
        },
        "overrides": []
      },
      "gridPos": {
        "h": 9,
        "w": 8,
        "x": 8,
        "y": 24
      },
      "id": 12,
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "pieType": "pie",
        "reduceOptions": {
          "calcs": [
            "lastNotNull"
          ],
          "fields": "",
          "values": false
        },
        "tooltip": {
          "mode": "single",
          "sort": "none"
        }
      },
      "pluginVersion": "9.1.3",
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "wAz-q1M4z"
          },
          "editorMode": "code",
          "expr": "(vector(5) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"RED\",TYPE=\"VDISK\"} > 0)) or\n(vector(4) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"ORANGE\",TYPE=\"VDISK\"} > 0)) or\n(vector(3) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"YELLOW\",TYPE=\"VDISK\"} > 0)) or\n(vector(2) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"BLUE\",TYPE=\"VDISK\"} > 0)) or vector(1) ",
          "legendFormat": "State",
          "range": true,
          "refId": "A"
        }
      ],
      "title": "Vdisks",
      "type": "piechart"
    },
    {
      "datasource": {
        "type": "prometheus",
        "uid": "wAz-q1M4z"
      },
      "description": "",
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "hideFrom": {
              "legend": false,
              "tooltip": false,
              "viz": false
            }
          },
          "mappings": [
            {
              "options": {
                "1": {
                  "color": "green",
                  "index": 4,
                  "text": "Good"
                },
                "2": {
                  "color": "blue",
                  "index": 3,
                  "text": "comes to a state of equilibrium"
                },
                "3": {
                  "color": "yellow",
                  "index": 2,
                  "text": "it is worth paying attention to"
                },
                "4": {
                  "color": "orange",
                  "index": 1,
                  "text": "one step to out of service"
                },
                "5": {
                  "color": "dark-red",
                  "index": 0,
                  "text": "out of service"
                }
              },
              "type": "value"
            }
          ]
        },
        "overrides": []
      },
      "gridPos": {
        "h": 9,
        "w": 8,
        "x": 16,
        "y": 24
      },
      "id": 13,
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "pieType": "pie",
        "reduceOptions": {
          "calcs": [
            "lastNotNull"
          ],
          "fields": "",
          "values": false
        },
        "tooltip": {
          "mode": "single",
          "sort": "none"
        }
      },
      "pluginVersion": "9.1.3",
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "wAz-q1M4z"
          },
          "editorMode": "code",
          "expr": "(vector(5) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"RED\",TYPE=\"NODES_SYNC\"} > 0)) or\n(vector(4) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"ORANGE\",TYPE=\"NODES_SYNC\"} > 0)) or\n(vector(3) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"YELLOW\",TYPE=\"NODES_SYNC\"} > 0)) or\n(vector(2) and on() (ydb_healthcheck{DOMAIN=\"$Domain\", STATUS=\"BLUE\",TYPE=\"NODES_SYNC\"} > 0)) or vector(1) ",
          "legendFormat": "State",
          "range": true,
          "refId": "A"
        }
      ],
      "title": "NodesSync",
      "type": "piechart"
    },
    {
      "datasource": {
        "type": "prometheus",
        "uid": "wAz-q1M4z"
      },
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "axisCenteredZero": false,
            "axisColorMode": "text",
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": "line",
            "fillOpacity": 0,
            "gradientMode": "none",
            "hideFrom": {
              "legend": false,
              "tooltip": false,
              "viz": false
            },
            "lineInterpolation": "linear",
            "lineWidth": 1,
            "pointSize": 5,
            "scaleDistribution": {
              "type": "linear"
            },
            "showPoints": "auto",
            "spanNulls": false,
            "stacking": {
              "group": "A",
              "mode": "none"
            },
            "thresholdsStyle": {
              "mode": "off"
            }
          },
          "mappings": [],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green"
              },
              {
                "color": "red",
                "value": 80
              }
            ]
          }
        },
        "overrides": []
      },
      "gridPos": {
        "h": 7,
        "w": 8,
        "x": 0,
        "y": 33
      },
      "id": 9,
      "options": {
        "legend": {
          "calcs": [],
          "displayMode": "list",
          "placement": "right",
          "showLegend": true
        },
        "tooltip": {
          "mode": "single",
          "sort": "none"
        }
      },
      "pluginVersion": "9.1.3",
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "wAz-q1M4z"
          },
          "editorMode": "code",
          "expr": "sum by (TYPE, MESSAGE) (ydb_healthcheck{DOMAIN=\"$Domain\",TYPE=\"PDISK\"}) > 0 or on() vector(0)",
          "legendFormat": "{{TYPE}} ({{MESSAGE}})",
          "range": true,
          "refId": "A"
        }
      ],
      "title": "State (Pdisks)",
      "type": "timeseries"
    },
    {
      "datasource": {
        "type": "prometheus",
        "uid": "wAz-q1M4z"
      },
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "axisCenteredZero": false,
            "axisColorMode": "text",
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": "line",
            "fillOpacity": 0,
            "gradientMode": "none",
            "hideFrom": {
              "legend": false,
              "tooltip": false,
              "viz": false
            },
            "lineInterpolation": "linear",
            "lineWidth": 1,
            "pointSize": 5,
            "scaleDistribution": {
              "type": "linear"
            },
            "showPoints": "auto",
            "spanNulls": false,
            "stacking": {
              "group": "A",
              "mode": "none"
            },
            "thresholdsStyle": {
              "mode": "off"
            }
          },
          "mappings": [],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green"
              },
              {
                "color": "red",
                "value": 80
              }
            ]
          }
        },
        "overrides": []
      },
      "gridPos": {
        "h": 7,
        "w": 8,
        "x": 8,
        "y": 33
      },
      "id": 15,
      "options": {
        "legend": {
          "calcs": [],
          "displayMode": "list",
          "placement": "right",
          "showLegend": true
        },
        "tooltip": {
          "mode": "single",
          "sort": "none"
        }
      },
      "pluginVersion": "9.1.3",
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "wAz-q1M4z"
          },
          "editorMode": "code",
          "expr": "sum by (TYPE, MESSAGE) (ydb_healthcheck{DOMAIN=\"$Domain\",TYPE=\"VDISK\"}) > 0 or on() vector(0)",
          "legendFormat": "{{TYPE}} ({{MESSAGE}})",
          "range": true,
          "refId": "A"
        }
      ],
      "title": "State (Vdisks)",
      "type": "timeseries"
    },
    {
      "datasource": {
        "type": "prometheus",
        "uid": "wAz-q1M4z"
      },
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "axisCenteredZero": false,
            "axisColorMode": "text",
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": "line",
            "fillOpacity": 0,
            "gradientMode": "none",
            "hideFrom": {
              "legend": false,
              "tooltip": false,
              "viz": false
            },
            "lineInterpolation": "linear",
            "lineWidth": 1,
            "pointSize": 5,
            "scaleDistribution": {
              "type": "linear"
            },
            "showPoints": "auto",
            "spanNulls": false,
            "stacking": {
              "group": "A",
              "mode": "none"
            },
            "thresholdsStyle": {
              "mode": "off"
            }
          },
          "mappings": [],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green"
              },
              {
                "color": "red",
                "value": 80
              }
            ]
          }
        },
        "overrides": []
      },
      "gridPos": {
        "h": 7,
        "w": 8,
        "x": 16,
        "y": 33
      },
      "id": 16,
      "options": {
        "legend": {
          "calcs": [],
          "displayMode": "list",
          "placement": "right",
          "showLegend": true
        },
        "tooltip": {
          "mode": "single",
          "sort": "none"
        }
      },
      "pluginVersion": "9.1.3",
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "wAz-q1M4z"
          },
          "editorMode": "code",
          "expr": "sum by (TYPE, MESSAGE) (ydb_healthcheck{DOMAIN=\"$Domain\",TYPE=~\"NODES_SYNC|TIME\"}) > 0 or on() vector(0)",
          "legendFormat": "{{TYPE}} ({{MESSAGE}})",
          "range": true,
          "refId": "A"
        }
      ],
      "title": "State (NodeSync)",
      "type": "timeseries"
    }
  ],
  "refresh": "5s",
  "schemaVersion": 37,
  "style": "dark",
  "tags": [],
  "templating": {
    "list": [
      {
        "current": {
          "selected": false,
          "text": "ru",
          "value": "ru"
        },
        "datasource": {
          "type": "prometheus",
          "uid": "wAz-q1M4z"
        },
        "definition": "label_values(DOMAIN)",
        "hide": 0,
        "includeAll": false,
        "label": "Домен",
        "multi": false,
        "name": "Domain",
        "options": [],
        "query": {
          "query": "label_values(DOMAIN)",
          "refId": "StandardVariableQuery"
        },
        "refresh": 1,
        "regex": "",
        "skipUrlSync": false,
        "sort": 0,
        "type": "query"
      }
    ]
  },
  "time": {
    "from": "now-1h",
    "to": "now"
  },
  "timepicker": {},
  "timezone": "",
  "title": "HC_test",
  "uid": "vFLzeJG4j",
  "version": 25,
  "weekStart": ""
}
  ```

</details>
