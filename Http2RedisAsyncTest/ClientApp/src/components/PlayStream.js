import React, { Component } from 'react';

export class PlayStream extends Component {
    static displayName = PlayStream.name;

  constructor(props) {
    super(props);
    this.state = { forecasts: [], loading: true };
  }

  componentDidMount() {
    this.populateWeatherData();
  }

  static renderLiveStream(liveStream) {
    return (
        <div>
            <div>
                <span>Camera ID: </span><span>{liveStream.cameraId}</span>
            </div>
            <div>
                <span>Stream URL: </span><span>{liveStream.streamUrl}</span>
            </div>
        </div>
    );
  }

  render() {
    let contents = this.state.loading
      ? <p><em>Loading...</em></p>
        : PlayStream.renderLiveStream(this.state.forecasts);

    return (
      <div>
        <h1 id="tabelLabel" >Play Stream</h1>
        <p>This component demonstrates fetching data from the server.</p>
        {contents}
      </div>
    );
  }

  async populateWeatherData() {
      const response = await fetch('https://localhost:7162/livestream?cameraId=CAM01');
    const data = await response.json();
    this.setState({ forecasts: data, loading: false });
  }
}
