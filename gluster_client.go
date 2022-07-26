package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"gluster_exporter/structs"

	"github.com/rs/zerolog/log"
)

func execGlusterCommand(arg ...string) (*bytes.Buffer, error) {
	stdoutBuffer := &bytes.Buffer{}
	argXML := append(arg, "--xml")
	log.Debug().Msgf("%s %s", GlusterCmd, strings.Join(argXML, " "))
	glusterExec := exec.Command(GlusterCmd, argXML...)
	glusterExec.Stdout = stdoutBuffer
	err := glusterExec.Run()

	if err != nil {
		log.Error().Msgf("tried to execute %v and got error: %v", arg, err)
		return stdoutBuffer, err
	}
	return stdoutBuffer, nil
}

func execMountCheck() (*bytes.Buffer, error) {
	stdoutBuffer := &bytes.Buffer{}
	mountCmd := exec.Command("mount", "-t", "fuse.glusterfs")

	mountCmd.Stdout = stdoutBuffer

	return stdoutBuffer, mountCmd.Run()
}

func execTouchOnVolumes(mountpoint string) (bool, error) {
	testFileName := fmt.Sprintf("%v/%v_%v", mountpoint, "gluster_mount.test", time.Now())
	_, createErr := os.Create(testFileName)
	if createErr != nil {
		return false, createErr
	}
	removeErr := os.Remove(testFileName)
	if removeErr != nil {
		return false, removeErr
	}
	return true, nil
}

// ExecVolumeInfo executes "gluster volume info" at the local machine and
// returns VolumeInfoXML struct and error
func ExecVolumeInfo() (structs.VolumeInfoXML, error) {
	args := []string{"volume", "info"}
	bytesBuffer, cmdErr := execGlusterCommand(args...)
	if cmdErr != nil {
		return structs.VolumeInfoXML{}, cmdErr
	}
	volumeInfo, err := structs.VolumeInfoXMLUnmarshall(bytesBuffer)
	if err != nil {
		log.Error().Msgf("Something went wrong while unmarshalling xml: %v", err)
		return volumeInfo, err
	}
	log.Debug().Msgf("%+v", volumeInfo)

	return volumeInfo, nil
}

// ExecVolumeList executes "gluster volume info" at the local machine and
// returns VolumeList struct and error
func ExecVolumeList() (structs.VolList, error) {
	args := []string{"volume", "list"}
	bytesBuffer, cmdErr := execGlusterCommand(args...)
	if cmdErr != nil {
		return structs.VolList{}, cmdErr
	}
	volumeList, err := structs.VolumeListXMLUnmarshall(bytesBuffer)
	if err != nil {
		log.Error().Msgf("Something went wrong while unmarshalling xml: %v", err)
		return volumeList.VolList, err
	}
	log.Debug().Msgf("%+v", volumeList)

	return volumeList.VolList, nil
}

// ExecPeerStatus executes "gluster peer status" at the local machine and
// returns PeerStatus struct and error
func ExecPeerStatus() (structs.PeerStatus, error) {
	args := []string{"peer", "status"}
	bytesBuffer, cmdErr := execGlusterCommand(args...)
	if cmdErr != nil {
		return structs.PeerStatus{}, cmdErr
	}
	peerStatus, err := structs.PeerStatusXMLUnmarshall(bytesBuffer)
	if err != nil {
		log.Error().Msgf("Something went wrong while unmarshalling xml: %v", err)
		return peerStatus.PeerStatus, err
	}
	log.Debug().Msgf("%+v", peerStatus)

	return peerStatus.PeerStatus, nil
}

// ExecVolumeProfileGvInfoCumulative executes "gluster volume {volume] profile info cumulative" at the local machine and
// returns VolumeInfoXML struct and error
func ExecVolumeProfileGvInfoCumulative(volumeName string) (structs.VolProfile, error) {
	args := []string{"volume", "profile", volumeName, "info", "cumulative"}
	bytesBuffer, cmdErr := execGlusterCommand(args...)
	if cmdErr != nil {
		return structs.VolProfile{}, cmdErr
	}
	volumeProfile, err := structs.VolumeProfileGvInfoCumulativeXMLUnmarshall(bytesBuffer)
	if err != nil {
		log.Error().Msgf("Something went wrong while unmarshalling xml: %v", err)
		return volumeProfile.VolProfile, err
	}
	log.Debug().Msgf("%+v", volumeProfile)
	return volumeProfile.VolProfile, nil
}

// ExecVolumeStatusAllDetail executes "gluster volume status all detail" at the local machine
// returns VolumeStatusXML struct and error
func ExecVolumeStatusAllDetail() (structs.VolumeStatusXML, error) {
	args := []string{"volume", "status", "all", "detail"}
	bytesBuffer, cmdErr := execGlusterCommand(args...)
	if cmdErr != nil {
		return structs.VolumeStatusXML{}, cmdErr
	}
	volumeStatus, err := structs.VolumeStatusAllDetailXMLUnmarshall(bytesBuffer)
	if err != nil {
		log.Error().Msgf("Something went wrong while unmarshalling xml: %v", err)
		return volumeStatus, err
	}
	log.Debug().Msgf("%+v", volumeStatus)
	return volumeStatus, nil
}

// ExecVolumeHealInfo executes volume heal info on host system and processes input
// returns (int) number of unsynced files
func ExecVolumeHealInfo(volumeName string) (int, error) {
	args := []string{"volume", "heal", volumeName, "info"}
	entriesOutOfSync := 0
	bytesBuffer, cmdErr := execGlusterCommand(args...)
	if cmdErr != nil {
		return -1, cmdErr
	}
	healInfo, err := structs.VolumeHealInfoXMLUnmarshall(bytesBuffer)
	if err != nil {
		log.Error().Err(err)
		return -1, err
	}
	log.Debug().Msgf("%+v", healInfo)

	for _, brick := range healInfo.HealInfo.Bricks.Brick {
		var count int
		var err error
		count, err = strconv.Atoi(brick.NumberOfEntries)
		if err != nil {
			log.Error().Err(err)
			return -1, err
		}
		entriesOutOfSync += count
	}
	log.Debug().Msgf("%+v", entriesOutOfSync)
	return entriesOutOfSync, nil
}

// ExecVolumeQuotaList executes volume quota list on host system and processes input
// returns QuotaList structs and errors
func ExecVolumeQuotaList(volumeName string) (structs.VolumeQuotaXML, error) {
	args := []string{"volume", "quota", volumeName, "list"}
	bytesBuffer, cmdErr := execGlusterCommand(args...)
	if cmdErr != nil {
		return structs.VolumeQuotaXML{}, cmdErr
	}
	volumeQuota, err := structs.VolumeQuotaListXMLUnmarshall(bytesBuffer)
	if err != nil {
		log.Error().Msgf("Something went wrong while unmarshalling xml: %v", err)
		return volumeQuota, err
	}
	log.Debug().Msgf("%+v", volumeQuota)
	return volumeQuota, nil
}
