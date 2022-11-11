import { segment_index } from '../media/segment_index.js';
import fs from 'fs';
import fse from 'fs-extra';
import logger from '../util/sp_logger.js';
import network_engine from '../net/network_engine.js';
import { proxyConf } from '../../proxy_conf.js';
import shell from 'shelljs';
import mergeFiles from 'merge-files';
import appRoot from 'app-root-path';
import path from 'path';
import error from '../util/error.js';
import assert from 'assert';
import { exit } from 'process';

var filePath = import.meta.url;

/**
 * Download all segments from segment index and decrypt them
 * by using their init data and our own API
 */
class segment_saver {
  /**
   * @param {segment_index} audioSegmentIndex target segment index from which
   * all audio segment files are downloaded.
   * @param {segment_index} videoSegmentIndex target segment index from which
   * all video segment files are downloaded.
   * @param {string} key decryption key derived from pssh in manifest and our API.
   * @param {string} keyId key Id
   * @param {string} decryptScript external script for decrypting segments.
   * @param {string} resultPath path to save completed segments.
   * @param {string} downloadPath path to download raw segments
   * @param {string} mergePath path to save merged segments
   * @param {number} endPlayTime average start time of live edge segment
   * @param {number} maxSegmentNum number of maximum segments in playlist buffer
   * @param {string} audioPLName name of playlist file in audio track
   * @param {string} videoPLName name of playlist file in video track
   * @param {object} lastSegmentURI URI object for the lastest segment processed in a loop
   * @param {boolean} manifestExpired true when manifest is expired.
   */
  constructor(audioSegmentIndex, videoSegmentIndex, key, keyId,
    decryptScript, resultPath, downloadPath, mergePath, endPlayTime, maxSegmentNum,
    audioPLName, videoPLName, lastSegmentURI, manifestExpired) {
    this.audioSegmentURIList_ = [];
    this.audioSegmentDurationList_ = [];
    this.videoSegmentURIList_ = [];
    this.videoSegmentDurationList_ = [];
    this.maxSegmentNum_ = maxSegmentNum;
    this.audioPLName_ = audioPLName;
    this.videoPLName_ = videoPLName;
    this.endPlayTime_ = endPlayTime;
    this.savePath_ = downloadPath;
    this.mergePath_ = mergePath;
    this.manifestExpired_ = manifestExpired;

    // initialize segment URI lists for audio and video tracks
    this.getSegmentInfoList_(audioSegmentIndex, true, lastSegmentURI.audio);
    this.getSegmentInfoList_(videoSegmentIndex, false, lastSegmentURI.video);

    // if (this.manifestExpired_) {
    //   console.log(audioSegmentIndex);
    // }

    if (lastSegmentURI.audio) {
      logger.sp_log(filePath, `lasteset URI: ${this.getDecimalIndexFromURI_(lastSegmentURI.audio)} ${(lastSegmentURI.audio).split('/').pop()}`);
    }
    if (lastSegmentURI.audio && this.audioSegmentURIList_.length > 1) {
      let firstNewSegmentIndex = this.getDecimalIndexFromURI_(this.audioSegmentURIList_[1]);
      let lastSegmentIndex = this.getDecimalIndexFromURI_(lastSegmentURI.audio);
      let offset = firstNewSegmentIndex - lastSegmentIndex;

      logger.sp_log(filePath, `Offest between iteration offset: ${offset} between ${lastSegmentIndex}-${firstNewSegmentIndex}.`);
      if (offset != 1) {
        logger.sp_error(filePath, `Dismatching segments`);
      }
    }

    this.decryptKey_ = key;
    this.keyId_ = keyId;
    this.decryptScript_ = decryptScript;
    if (!fs.existsSync(resultPath)) {
      logger.sp_warn(filePath, "Default result path %s doesn't exist, creating new one...",
        resultPath);
      fs.mkdirSync(resultPath);
    }
    if (!fs.existsSync(this.savePath_)) {
      logger.sp_warn(filePath, "Default download path %s doesn't exist, creating new one...",
        this.savePath_);
      fs.mkdirSync(this.savePath_);
    }
    if (!fs.existsSync(this.mergePath_)) {
      logger.sp_warn(filePath, "Path %s where combined segments will be saved doesn't exist, creating new one...",
        this.mergePath_);
      fs.mkdirSync(this.mergePath_);
    }

    this.resultPath_ = resultPath;
  }

  clearReference_() {
    this.audioSegmentURIList_ = [];
    this.audioSegmentDurationList_ = [];
    this.videoSegmentURIList_ = [];
    this.videoSegmentDurationList_ = [];
    this.decryptKey_ = undefined;
    this.keyId_ = undefined;
    this.decryptScript_ = undefined;
    this.resultPath_ = undefined;
  }

  /**
   * Extract segment uri list and duration list from segment index of
   * a certain track(audio or video).
   * @param {segment_index} segmentIndex target segment index
   * @param {boolean} isAudio true in case of audio
   * @param {string} lastSegmentURI URI of the lastest segment
   *  processed in a loop
   */
  getSegmentInfoList_(segmentIndex, isAudio, lastSegmentURI) {
    let segmentURIList = [];
    let segmentDurationList = [];

    // Add URI of init segment.
    let refArray = segmentIndex.references;
    if (refArray.length == 0) {
      return;
    }
    segmentURIList.push(refArray[0].initSegmentReference.getUris()[0]);
    // Init segment's duration is 0.
    segmentDurationList.push(0);

    let foundLatestName = false;
    let totalList = [];
    let totalSegList = [];


    segmentIndex.forEachTopLevelReference((ref) => {
      let segURI = ref.getUrisInner()[0];
      let segDuration = ref.endTime - ref.startTime;
      totalList.push(segURI);
      totalSegList.push(segDuration);
      if (foundLatestName) {
        segmentURIList.push(segURI);
        segmentDurationList.push(segDuration);
      }

      if (segURI == lastSegmentURI) {
        foundLatestName = true;
      }
    });

    // If there aren't any segments with lastSegmentURI,
    // then take last max_segment_number of segments from
    // the totalLists
    if (segmentURIList.length == 1 && totalList.length >= 0) {
      let firstIndex = Math.max(0, totalList.length - this.maxSegmentNum_);
      segmentURIList = segmentURIList.concat(totalList.slice(firstIndex));
      segmentDurationList = segmentDurationList.concat(totalSegList.slice(firstIndex));
    }

    if (isAudio) {
      this.audioSegmentURIList_ = segmentURIList;
      this.audioSegmentDurationList_ = segmentDurationList;
    } else {
      this.videoSegmentURIList_ = segmentURIList;
      this.videoSegmentDurationList_ = segmentDurationList;
    }
  }

  getDecimalIndexFromURI_(segmentURI) {
    if (segmentURI != null) {
      var segmentName = segmentURI.split('/').pop();
      var nameOnly = path.basename(segmentName, path.extname(segmentName));
      return parseInt('0x' + nameOnly);
    } else {
      return 0;
    }
  }

  async processSegments(audioMediaPLTemplate, videoMediaPLTemplate, updateDuration) {
    return await this.processSegmentList(this.audioSegmentURIList_, this.videoSegmentURIList_,
      this.audioSegmentDurationList_, this.videoSegmentDurationList_,
      audioMediaPLTemplate, videoMediaPLTemplate, this.decryptKey_, this.keyId_, updateDuration);
  }

  async processSegmentList(audioSegmentURIList, videoSegmentURIList,
    audioSegmentDurationList, videoSegmentDurationList,
    audioMediaPLTemplate, videoMediaPLTemplate, key, keyId, updateDuration) {
    let i = 0;
    
    if (this.audioSegmentURIList_.length != this.videoSegmentURIList_.length) {
      let sizeOffest = this.videoSegmentURIList_.length - this.audioSegmentURIList_.length;
      for (let i = 0; i < Math.abs(sizeOffest); i++) {
        if (sizeOffest > 0) {
          this.videoSegmentURIList_.pop();
        } else {
          this.audioSegmentURIList_.pop();
        }
      }
    }

    // total segments number = url list length - 1 (init segment isn't involved)
    logger.sp_log(filePath, `Processing ${audioSegmentURIList.length - 1} segment(s) for audio, ${videoSegmentURIList.length - 1} segment(s) for video...`);

    let lastAudioURI = null;
    let lastVideoURI = null;

    // check how long it take to process one segment per track
    let checkStart = Date.now();

    for (let i = 0; i < audioSegmentURIList.length; i++) {
      var segmentUrl;
      var segmentDuration;
      var pathSuffix;
      var initFile = "init.mp4";
      var mediaPlaylistTemplate;
      var playlistName;
      var mediaPlaylistPath;
      // traverse both audio and video segments
      for (let j = 0; j < 2; j++) {
        if (j == 0) {
          segmentUrl = audioSegmentURIList[i];
          segmentDuration = audioSegmentDurationList[i];
          pathSuffix = "audio/";
          mediaPlaylistTemplate = audioMediaPLTemplate;
          playlistName = this.audioPLName_;
          lastAudioURI = segmentUrl;
        } else {
          segmentUrl = videoSegmentURIList[i];
          segmentDuration = videoSegmentDurationList[i];
          pathSuffix = "video/";
          mediaPlaylistTemplate = videoMediaPLTemplate;
          playlistName = this.videoPLName_;
          lastVideoURI = segmentUrl;
        }
        mediaPlaylistPath = this.resultPath_ + pathSuffix + playlistName;

        var segmentName = segmentUrl.split('/').pop();
        let saveName = this.savePath_ + pathSuffix + segmentName;

        await network_engine.socks5_http_download(segmentUrl, saveName, proxyConf);

        // Combine each segments with init one.
        if (segmentName != initFile) {
          const inputPathList = [];
          inputPathList.push(this.savePath_ + pathSuffix + initFile);
          inputPathList.push(saveName);
          let mergeFileName = this.mergePath_ + pathSuffix + segmentName;
          const status = await mergeFiles(inputPathList, mergeFileName);
          if (status) {
            // Decrypt the segment.
            const nameOnly = path.basename(segmentName, path.extname(segmentName));
            const resultFileRelativePath = pathSuffix + nameOnly + ".mp4";
            let decryptCommand = this.decryptScript_ + " " + keyId + " " +
              key + " " + mergeFileName + " " + this.resultPath_ +
              resultFileRelativePath + " " + appRoot.path + " " + pathSuffix.slice(0, -1);
            if (shell.exec(decryptCommand).code !== 0) {
              logger.sp_error(filePath, "Decrypting failed.");
              throw new error(
                error.Severity.CRITICAL,
                error.Category.SEGMENT,
                error.Code.SEGMENT_MANIPULATION_FAILED);
            }
            logger.sp_debug(filePath, `${pathSuffix.slice(0, -1)} ${nameOnly} Decrypted.`);

            // As soon as the new segments are decrypted and added, then
            // update the media playlist also.

            let newSegmentUri = nameOnly + ".mp4";
            let segmentTemplate = `#EXTINF:${segmentDuration},
${newSegmentUri}`;

            // Get the total number of segments and check if
            // it exceeds the max segment number. If yes, 
            // then delete old ones.
            let segmentItemList = [];
            mediaPlaylistTemplate.map((item) => {
              if (item.includes('#EXTINF:')) {
                segmentItemList.push(item);
              }
            });
            if (segmentItemList.length == this.maxSegmentNum_) {
              const oldItem = segmentItemList.shift();
              mediaPlaylistTemplate.splice(mediaPlaylistTemplate.indexOf(oldItem), 1);

              // Delete respective file really also, not just manifest item
              let oldFilename = this.resultPath_ + pathSuffix + oldItem.split('\n').pop();
              fs.unlinkSync(oldFilename);

              // get the name of first item now to update media sequence
              segmentItemList.push(segmentTemplate);

              let regEx = RegExp(/#EXT-X-MEDIA-SEQUENCE:\d+/i);
              let oldSequenceNumber = parseInt((regEx.exec(mediaPlaylistTemplate[0])[0]).replace(/^\D+/g, ''));
              let newSequenceNumber = oldSequenceNumber + 1;
              const mediaSequenceStr = `#EXT-X-MEDIA-SEQUENCE:${newSequenceNumber}`;
              let newItem = mediaPlaylistTemplate[0].replace(/#EXT-X-MEDIA-SEQUENCE:\d+/i, mediaSequenceStr);
              mediaPlaylistTemplate[0] = newItem;
            }

            mediaPlaylistTemplate.push(segmentTemplate);
            fs.writeFileSync(mediaPlaylistPath, mediaPlaylistTemplate.join('\n'));


          } else {
            logger.sp_error(filePath, "Failed to combine segments");
            throw new error(
              error.Severity.CRITICAL,
              error.Category.SEGMENT,
              error.Code.SEGMENT_MANIPULATION_FAILED);
          }
        }
      }
    }

    // Delete downloaded segments and combined ones.
    fse.emptyDirSync(this.savePath_ + "audio");
    fse.emptyDirSync(this.savePath_ + "video");
    fse.emptyDirSync(this.mergePath_ + "audio");
    fse.emptyDirSync(this.mergePath_ + "video");

    // if processing period is less than segment update period defined in manifest,
    // sleep for timeoffest(segment update period - processing period)
    let processPeriod = Date.now() - checkStart - updateDuration;
    logger.sp_debug(filePath, `${processPeriod / 1000} second(s) elapsed`);
    let segmentDurationMiliSec = segmentDuration * 1000;
    if (processPeriod < segmentDurationMiliSec) {
      let sleepingDuration = segmentDurationMiliSec - processPeriod;
      logger.sp_debug(filePath, `Sleeping for ${sleepingDuration / 1000}s...`);
      await this.sleep_(sleepingDuration);
    }

    this.clearReference_();

    return {
      audio: lastAudioURI,
      video: lastVideoURI
    };
  }

  sleep_(millis) {
    return new Promise(resolve => setTimeout(resolve, millis));
  }
}

export default segment_saver;