var xray = require('x-ray');
var async = require('async');
var moment = require('moment');

var SeLogerScrapper = {};
module.exports = SeLogerScrapper;

SeLogerScrapper.run = function run(options, progress, complete) {

    getIndex(function (err, index) {
        var countAnnoncesDone = 0;
        var countAnnonces = index.length;
        var annonces = [];
        async.eachSeries(index, function (item, callback) {
            getAnnonce(item, function (err, annonce) {
                annonces.push(annonce);
                countAnnoncesDone++;
                progress(countAnnoncesDone / countAnnonces);
                callback(err, annonce);
            });
        }, function (err, result) {
            complete(err, annonces);
        })
    });

    function getIndex(complete) {

        var index = [];

        var departements = [];
        for (var i = 1; i <= 10; i++) {
            departements.push(i);
            if (i <= 8) departements.push(970 + i);
        }

        async.eachSeries(departements, function (departement, callback) {
            console.log('[seloger] téléchargement index pour departement', departement, '...');
            xray('http://www.seloger.com/list.htm?cp=' + departement + '&idtt=2&naturebien=1')
                .select([{
                    $root: 'div.listing_infos > h2 > a',
                    lien: '[href]',
                    titre: ''
                }])
                .paginate('a.pagination_next[href]')
                .limit(1)
                .format(function (obj) {
                    obj.titre = obj.titre.split(', ')[0];
                    obj.site_id = obj.lien.match(/(.*\/)([0-9]+)(\.htm.*)/)[2];
                    return obj;
                })
                .run(function (err, result) {
                    index = index.concat(result);
                    console.log('[seloger] téléchargement index pour departement', departement, 'done');
                    callback(err, result)
                });
        }, function (err, result) {
            complete(err, index);
        });
    }

};


function getAnnonce(item, complete) {
    xray(item.lien)
        .select({
            auteur: '.agence-bloc-r .agence-title',
            prix: '#price',
            ville: '#detail .detail-subtitle span',
            temp_criterias_1_data: '#detail li:nth-child(1)',
            temp_criterias_2_data: '#detail li:nth-child(2)',
            temp_criterias_3_data: '#detail li:nth-child(3)',
            temp_criterias_4_data: '#detail li:nth-child(4)',
            temp_criterias_5_data: '#detail li:nth-child(5)',
            temp_criterias_6_data: '#detail li:nth-child(6)',
            temp_criterias_7_data: '#detail li:nth-child(7)',
            temp_criterias_8_data: '#detail li:nth-child(8)',
            temp_criterias_9_data: '#detail li:nth-child(9)',
            temp_criterias_10_data: '#detail li:nth-child(10)',
            temp_criterias_11_data: '#detail li:nth-child(11)',
            temp_criterias_12_data: '#detail li:nth-child(12)',
            temp_criterias_13_data: '#detail li:nth-child(13)',
            temp_criterias_14_data: '#detail li:nth-child(14)',
            temp_criterias_15_data: '#detail li:nth-child(15)',
            temp_criterias_16_data: '#detail li:nth-child(16)',
            temp_criterias_17_data: '#detail li:nth-child(17)',
            temp_criterias_18_data: '#detail li:nth-child(18)',
            temp_criterias_19_data: '#detail li:nth-child(19)',
            temp_criterias_20_data: '#detail li:nth-child(20)',
            texte: '.description',
            reference: '.description_ref',
            thumb_1: 'ul#slider2.carrousel__liste > li:nth-child(1) > a > img[src]',
            thumb_2: 'ul#slider2.carrousel__liste > li:nth-child(2) > a > img[src]',
            thumb_3: 'ul#slider2.carrousel__liste > li:nth-child(3) > a > img[src]',
            thumb_4: 'ul#slider2.carrousel__liste > li:nth-child(4) > a > img[src]',
            thumb_5: 'ul#slider2.carrousel__liste > li:nth-child(5) > a > img[src]',
            thumb_6: 'ul#slider2.carrousel__liste > li:nth-child(6) > a > img[src]',
            thumb_7: 'ul#slider2.carrousel__liste > li:nth-child(7) > a > img[src]',
            thumb_8: 'ul#slider2.carrousel__liste > li:nth-child(8) > a > img[src]',
            thumb_9: 'ul#slider2.carrousel__liste > li:nth-child(9) > a > img[src]',
            thumb_10: 'ul#slider2.carrousel__liste > li:nth-child(10) > a > img[src]',
            thumb_11: 'ul#slider2.carrousel__liste > li:nth-child(11) > a > img[src]',
            thumb_12: 'ul#slider2.carrousel__liste > li:nth-child(12) > a > img[src]',
            thumb_13: 'ul#slider2.carrousel__liste > li:nth-child(13) > a > img[src]',
            thumb_14: 'ul#slider2.carrousel__liste > li:nth-child(14) > a > img[src]',
            thumb_15: 'ul#slider2.carrousel__liste > li:nth-child(15) > a > img[src]'
        })
        .format(function (obj) {
            if (obj) {
                obj.site = 'seloger';
                obj.timestamp = moment().unix() * 1000;
                obj.titre = item.titre;
                obj.pro = true;
                obj.frais_agence_inclus = true;
                obj.lien = item.lien;
                obj.type = obj.lien.split('/')[5];
                if (obj.prix) obj.prix = obj.prix ? obj.prix.replace(/[^0-9]/g, '') : null;
                if (obj.texte) obj.texte = obj.texte.replace(/\n/g, ' ').replace(/\s{2,}/g, ' ').trim();
                obj.ville = obj.ville.match(/(à )(.*)/)[2].trim();
                obj.reference = obj.reference.match(/(Réf.: +)(.*)/)[2].trim();
                obj.auteur = obj.auteur.replace(/[\n\t]/, '').replace(/\s{2,}/g, ' ').trim();
                // nettoyage caractéristiques
                for (var i = 1; i <= 20; i++) {
                    var d = obj['temp_criterias_' + i + '_data'];
                    if (d) {
                        d = d.trim();
                        if (d.indexOf('Surface de') > -1) {
                            var temp = d.match(/.*Surface\sde\s([0-9,]+)\s(\S+).*/);
                            obj.surface = temp[1] * (temp[2] === 'ha' ? 10000 : 1);
                        } else if (d.indexOf('Etages') > -1) {
                            obj.etages = d.match(/.*([0-9]+)\sEtages.*/)[1];
                        } else if (d.indexOf('Etage') > -1) {
                            obj.etage = d.match(/.*([0-9A-Za-z]+)\sEtage.*/)[1];
                        } else if (d.indexOf('Pièces') > -1) {
                            obj.pieces = d.match(/.*([0-9]+)\sPièces.*/)[1];
                        } else if (d.indexOf('Chambres') > -1) {
                            obj.chambres = d.match(/.*([0-9]+)\sChambres.*/)[1];
                        } else if (d.indexOf('Parking') > -1) {
                            obj.parking = d.match(/.*([0-9]+)\sParking.*/)[1];
                        } else if (d.indexOf('Ascenseur') > -1) {
                            obj.ascenseur = true;
                        } else if (d.indexOf('Psicine') > -1) {
                            obj.piscine = d.match(/.*([0-9]+)\sPiscine.*/)[1];
                        } else if (d.indexOf('Terrain de') > -1) {
                            var temp = d.match(/.*de\s([0-9,]+)\s(\S+).*/);
                            obj.surface_terrain = temp[1] * (temp[2] === 'ha' ? 10000 : 1);
                        } else if (d.indexOf('DPE') > -1) {
                            obj.classe_energie = d.match(/.*DPE\s:\s([A-Z]{1,1}).*/)[1];
                        } else if (d.indexOf('GES') > -1) {
                            obj.GES = d.match(/.*GES\s:\s([A-Z]{1,1}).*/)[1];
                        }
                    }
                    delete obj['temp_criterias_' + i + '_data'];
                }
                // nettoyage thumbnails
                var thumbs = [];
                for (var i = 1; i <= 15; i++) {
                    d = obj['thumb_' + i];
                    if (d) {
                        thumbs.push(d.match(/(http:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)*\/?/)[0]);
                        delete obj['thumb_' + i];
                    }
                }
                if (thumbs.length) obj.thumbs = thumbs;
            }
            return obj;
        })
        .run(function (err, result) {
            complete(err, result);
        });
}

SeLogerScrapper.run(
    {},
    function progress(percent) {
        console.log('progress: ' + (percent * 100).toFixed(1) + '%');
    },
    function complete(err, result) {
        //console.log('index', index, 'length', index.length);
        console.log('done, err', err, 'result', result, 'length', result.length);
    }
);

